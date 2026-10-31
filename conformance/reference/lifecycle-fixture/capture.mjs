import assert from "node:assert/strict"
import { execFile } from "node:child_process"
import { createRequire } from "node:module"
import { cp, mkdir, mkdtemp, readFile, readdir, rm, symlink, writeFile } from "node:fs/promises"
import { tmpdir } from "node:os"
import { join, resolve } from "node:path"
import { fileURLToPath, pathToFileURL } from "node:url"
import { promisify } from "node:util"
import { sha256 } from "../fixture/harness.mjs"
import { verifyReferenceRoot, parseArguments } from "../fixture/capture.mjs"
import { collectRuntimeArtifacts, createRuntimeArtifactReceipt } from "../fixture/runtime-artifacts.mjs"
import { parseLifecycleSse, projectLifecycleObservation } from "./projection.mjs"

const fixtureRoot = fileURLToPath(new URL(".", import.meta.url))
const expectedInventory = "71e2663947013ddf3dbac74abcc1465b1364ae946a33256a2807c8ec35163018"
const exec = promisify(execFile)
const delay = ms => new Promise(resolve => setTimeout(resolve, ms))
async function bounded(promise, description, ms = 10_000) {
  let timer
  try { return await Promise.race([promise, new Promise((_, reject) => { timer = setTimeout(() => reject(new Error(description + " deadline exceeded")), ms) })]) }
  finally { clearTimeout(timer) }
}
async function hashFiles(root, prefix = "") {
  const files = []
  for (const entry of (await readdir(join(root, prefix), { withFileTypes: true })).sort((a, b) => a.name < b.name ? -1 : a.name > b.name ? 1 : 0)) {
    const path = prefix ? `${prefix}/${entry.name}` : entry.name
    if (entry.isDirectory()) files.push(...await hashFiles(root, path))
    else if (entry.isFile()) {
      const bytes = await readFile(join(root, path))
      files.push({ path, sha256: sha256(bytes), bytes: bytes.length })
    }
  }
  return files
}
async function dependencyVersions(referenceRoot) {
  const versions = {}
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
  ]) versions[name] = JSON.parse(await readFile(join(referenceRoot, path), "utf8")).version
  return versions
}

export async function capture({ referenceRoot, output }) {
  assert.equal(process.version, "v24.20.0", "Retained qualification runtime version required")
  const reference = await verifyReferenceRoot(referenceRoot)
  assert.equal((await exec("git", ["-C", referenceRoot, "status", "--porcelain"])).stdout, "", "Pinned source checkout must be clean")
  const artifacts = await createRuntimeArtifactReceipt({ referenceRoot, observationTiming: "capture_time_inventory" })
  assert.equal(artifacts.files.length, 385, "Only the retained scoped build is eligible")
  assert.equal(artifacts.inventory_sha256, expectedInventory, "Retained scoped build bytes changed")
  const retainedInventory = JSON.parse(await readFile(new URL("../recordings/2026-10-01-grants/runtime-artifacts.json", import.meta.url), "utf8"))
  assert.equal(sha256(JSON.stringify(artifacts.files)), sha256(JSON.stringify(retainedInventory.files)), "Every retained scoped artifact must match")
  const reusedBuild = await readFile(new URL("../recordings/2026-10-01-grants/build-provenance.json", import.meta.url))
  const runtimeEntry = "packages/cli/dist/lib/dev/runtime-fetch-handler.js"
  const runtimeBytes = await readFile(join(referenceRoot, runtimeEntry))
  const require = createRequire(join(referenceRoot, "packages/cli/package.json"))
  const { BaseChatModel } = await import(pathToFileURL(require.resolve("@langchain/core/language_models/chat_models")))
  const { AIMessage, isToolMessage } = await import(pathToFileURL(require.resolve("@langchain/core/messages")))
  const { seedModelImporter } = await import(pathToFileURL(join(referenceRoot, "packages/langchain/dist/index.js")))
  const { createRuntimeFetchHandler } = await import(pathToFileURL(join(referenceRoot, runtimeEntry)))
  const fixtureFiles = await hashFiles(fixtureRoot)
  const fixtureRevision = sha256(JSON.stringify(fixtureFiles))
  const temporaryRoot = await mkdtemp(join(tmpdir(), "b4-lifecycle-reference-"))
  const appRoot = join(temporaryRoot, "app")
  const ungatedRoot = join(temporaryRoot, "ungated-app")
  const barriers = join(temporaryRoot, "barriers")
  const ledgerPath = join(temporaryRoot, "effects.log")
  await mkdir(barriers)
  await writeFile(ledgerPath, "")
  for (const root of [appRoot, ungatedRoot]) {
    await cp(join(fixtureRoot, "app"), root, { recursive: true })
    await symlink(join(referenceRoot, "node_modules"), join(root, "node_modules"), "dir")
  }
  const previousEnvironment = new Map(["B4_LIFECYCLE_REFERENCE_LEDGER", "B4_LIFECYCLE_REFERENCE_BARRIERS"].map(name => [name, process.env[name]]))
  process.env.B4_LIFECYCLE_REFERENCE_LEDGER = ledgerPath
  process.env.B4_LIFECYCLE_REFERENCE_BARRIERS = barriers
  for (const name of ["OPENAI_API_KEY", "OPENAI_BASE_URL", "LANGSMITH_API_KEY", "LANGCHAIN_API_KEY", "LANGSMITH_TRACING", "LANGCHAIN_TRACING_V2"])
    delete process.env[name]
  let modelCalls = 0
  let importedModelPackages = 0
  class DeterministicChatModel extends BaseChatModel {
    constructor(options) {
      assert.equal(JSON.stringify(Object.keys(options).sort()), JSON.stringify(["maxRetries", "model"]))
      super({})
    }
    _llmType() { return "independent-local-lifecycle-reference" }
    bindTools() { return this }
    async _generate(messages) {
      modelCalls++
      const message = messages.some(isToolMessage)
        ? new AIMessage({ id: "fixture-ai-final", content: "Local decision recorded." })
        : new AIMessage({ id: "fixture-ai-proposal", content: "", tool_calls: [{ id: "fixture-call-park", name: "localDecision", args: { label: "park" }, type: "tool_call" }] })
      assert.ok(messages.some(isToolMessage) || messages.at(-1)?.content === "request local decision", "Unexpected local model input")
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
  globalThis.fetch = async () => { externalFetchAttempts++; throw new Error("Lifecycle fixture forbids external fetch calls") }
  let handler
  let ungatedHandler
  let profile = "owner_policy"
  let policyHold
  const observations = []
  const assertions = []
  const collectors = []
  const controllers = []
  const heldKeys = new Set()
  const ledger = async () => (await readFile(ledgerPath, "utf8")).split("\n").filter(Boolean).map(line => JSON.parse(line))
  const release = async key => writeFile(join(barriers, key + ".release"), "released\n")
  const entered = async key => {
    heldKeys.add(key)
    await bounded((async () => { while (!(await ledger()).some(entry => entry.key === key && entry.phase === "entered")) await delay(5) })(), "effect entry")
  }
  const drained = async key => {
    await bounded((async () => { while (!(await ledger()).some(entry => entry.key === key && entry.phase === "exited")) await delay(5) })(), "effect quiescence")
    // Actual subsequent admission below verifies the slot; this short settling
    // interval only lets the application's resolved promise unwind its iterator.
    await delay(30)
  }
  const threadAccess = {
    fallback: ({ headers, action, operation, thread }) => {
      if (headers["x-fixture-caller"] !== "owner") return { decision: "deny" }
      if (action === "create") return { decision: "allow", stamp: { owner: "owner" } }
      // Missing-resource probes from the fixture owner reach natural endpoint
      // behavior. Foreign actors remain denied before existence/activity checks.
      if (thread && thread.access?.owner !== "owner") return { decision: "deny" }
      if (operation === "thread.cancel" && headers["x-fixture-hold-policy"] === "yes") {
        assert.ok(policyHold, "Unexpected asynchronous policy request")
        policyHold.entered()
        return policyHold.promise.then(() => ({ decision: "allow" }))
      }
      return { decision: "allow" }
    },
  }
  const open = async () => {
    handler = await createRuntimeFetchHandler({ appRoot, threadAccess, drainDeadlineMs: 1_000, apSseHeartbeatIntervalMs: 60_000, apAttachMaxViewers: 1 })
  }
  async function begin(id, method, path, body, options = {}) {
    assert.ok(!observations.some(entry => entry.id === id), "Duplicate fixture observation identifier")
    const text = body === undefined ? undefined : JSON.stringify(body)
    const controller = new AbortController()
    controllers.push(controller)
    const request = new Request(`http://reference.invalid${path}`, {
      method, signal: controller.signal,
      headers: { "x-fixture-caller": options.caller ?? "owner", ...(options.holdPolicy ? { "x-fixture-hold-policy": "yes" } : {}), ...(text === undefined ? {} : { "content-type": "application/json" }) },
      ...(text === undefined ? {} : { body: text }),
    })
    const observation = { id, sequence: observations.length + 1, profile, evidence_classification: "runtime_observation", request: {
      method, path, headers: [...request.headers.entries()], ...(text === undefined ? {} : { body_utf8: text, body_base64: Buffer.from(text).toString("base64"), body_sha256: sha256(text) }),
    } }
    observations.push(observation)
    let stateSeen
    const firstState = new Promise(resolve => { stateSeen = resolve })
    const activeHandler = options.ungated ? ungatedHandler : handler
    const responsePromise = activeHandler.fetch(request)
    const completed = bounded((async () => {
      const response = await responsePromise
      const chunks = []
      const reader = response.body?.getReader()
      if (reader) {
        for (;;) {
          const next = await reader.read()
          if (next.done) break
          chunks.push(Buffer.from(next.value))
          if (response.headers.get("content-type")?.includes("text/event-stream")) {
            const blocks = parseLifecycleSse(Buffer.concat(chunks).toString("utf8"))
            if (blocks.some(block => block.kind === "event" && block.event === "state")) stateSeen()
          }
        }
      }
      const bytes = Buffer.concat(chunks)
      const raw = bytes.toString("utf8")
      observation.response = {
        status: response.status, status_text: response.statusText, headers: [...response.headers.entries()],
        body_base64: bytes.toString("base64"), body_utf8: raw, body_sha256: sha256(bytes),
        capture: { complete: !options.viewerAbort, cut: options.viewerAbort ? "viewer_abort" : "eof" },
        ...(response.headers.get("content-type")?.includes("text/event-stream") ? { sse_blocks: parseLifecycleSse(raw), sse_frames: parseLifecycleSse(raw).filter(block => block.kind === "event").map(({ kind: _, ...frame }) => frame) } : {}),
      }
      observation.ledger = await ledger()
      observation.model_calls = modelCalls
      observation.projection = projectLifecycleObservation(observation)
      return observation
    })(), `response ${id}`)
    // Prevent an unhandled rejection if a later fixture assertion fails before
    // the already-started response collector is awaited during cleanup.
    completed.catch(() => {})
    collectors.push(completed)
    return { completed, firstState, abort: () => controller.abort(new Error("Fixture attached viewer departed")) }
  }
  const request = async (...args) => (await begin(...args)).completed
  const status = (observation, expected) => assert.equal(observation.response.status, expected, `Unexpected status for ${observation.id}`)
  const bodyOf = observation => JSON.parse(observation.response.body_utf8)
  const events = observation => observation.response.sse_frames?.map(frame => frame.event) ?? []
  const outputOf = observation => observation.response.sse_frames?.find(frame => frame.event === "done")?.data?.output
  const held = key => ({ route: "/held#workflow", input: { key } })
  const immediate = label => ({ route: "/immediate#workflow", input: { label } })
  async function absence(prefix, options = {}) {
    status(await request(`${prefix}.cancel`, "POST", "/threads/missing/cancel", undefined, options), 404)
    status(await request(`${prefix}.delete`, "DELETE", "/threads/missing", undefined, options), 204)
    status(await request(`${prefix}.state`, "GET", "/threads/missing/state", undefined, options), 404)
    status(await request(`${prefix}.attach`, "GET", "/threads/missing/runs/stream", undefined, options), 404)
  }
  try {
    await open()
    await absence("missing")
    for (const [name, method, path, expected] of [["cancel", "POST", "/threads/missing/cancel", 403], ["delete", "DELETE", "/threads/missing", 403], ["state", "GET", "/threads/missing/state", 404], ["attach", "GET", "/threads/missing/runs/stream", 404]])
      status(await request(`missing.foreign-${name}`, method, path, undefined, { caller: "other" }), expected)
    assertions.push("owner_missing_resource_policy_reaches_natural_endpoints_foreign_actor_is_denied")

    const idle = await request("idle.create", "POST", "/threads", { metadata: { purpose: "lifecycle fixture" } })
    status(idle, 200)
    const idleId = bodyOf(idle).thread_id
    status(await request("idle.thread", "GET", `/threads/${idleId}`), 200)
    status(await request("idle.cancel", "POST", `/threads/${idleId}/cancel`), 409)
    status(await request("idle.state", "GET", `/threads/${idleId}/state`), 404)
    status(await request("idle.attach", "GET", `/threads/${idleId}/runs/stream`), 409)
    status(await request("idle.delete", "DELETE", `/threads/${idleId}`), 204)
    status(await request("idle.deleted-thread", "GET", `/threads/${idleId}`), 404)
    assertions.push("idle_thread_deletion_is_real_and_idle_attach_has_no_recorded_route")

    const completedThread = "t-lifecycle-completed"
    status(await request("completed.run", "POST", `/threads/${completedThread}/runs/wait`, immediate("completed")), 200)
    status(await request("completed.thread", "GET", `/threads/${completedThread}`), 200)
    status(await request("completed.cancel", "POST", `/threads/${completedThread}/cancel`), 409)
    status(await request("completed.state", "GET", `/threads/${completedThread}/state`), 404)
    const completedAttach = await request("completed.attach", "GET", `/threads/${completedThread}/runs/stream`)
    assert.equal(JSON.stringify(events(completedAttach)), JSON.stringify(["state", "done"]))
    assert.ok(completedAttach.response.sse_blocks.some(block => block.kind === "retry" && block.retry_ms >= 1500 && block.retry_ms <= 2500))
    await handler.close(); handler = undefined; await open()
    const completedReopen = await request("completed.attach-after-reopen", "GET", `/threads/${completedThread}/runs/stream`)
    assert.equal(JSON.stringify(events(completedReopen)), JSON.stringify(["state", "done"]))
    status(await request("completed.delete", "DELETE", `/threads/${completedThread}`), 204)
    assertions.push("settled_plain_workflow_has_no_checkpoint_and_durable_attach_retry_survives_graceful_reopen")

    const parkedThread = "t-lifecycle-parked"
    const park = await request("parked.run", "POST", `/threads/${parkedThread}/runs/stream`, { route: "/park#agent", input: { messages: [{ role: "user", content: "request local decision" }] } })
    assert.ok(events(park).includes("interrupt"), "Real permission gate must park")
    const pendingBefore = await request("parked.pending", "GET", `/threads/${parkedThread}/pending_interrupts`)
    assert.equal(bodyOf(pendingBefore).interrupts.length, 1)
    const stateBefore = await request("parked.state", "GET", `/threads/${parkedThread}/state`)
    status(stateBefore, 200)
    const parkedAttach = await request("parked.attach", "GET", `/threads/${parkedThread}/runs/stream`)
    assert.equal(parkedAttach.response.sse_frames[0].data.interrupts.length, 1)
    status(await request("parked.cancel", "POST", `/threads/${parkedThread}/cancel`), 409)
    for (const [name, method, path, expected] of [["delete", "DELETE", `/threads/${parkedThread}`, 403], ["cancel", "POST", `/threads/${parkedThread}/cancel`, 403], ["state", "GET", `/threads/${parkedThread}/state`, 404], ["attach", "GET", `/threads/${parkedThread}/runs/stream`, 404]])
      status(await request(`parked.foreign-${name}`, method, path, undefined, { caller: "other" }), expected)
    await handler.close(); handler = undefined; await open()
    const pendingAfter = await request("parked.pending-after-reopen", "GET", `/threads/${parkedThread}/pending_interrupts`)
    assert.equal(pendingBefore.response.body_sha256, pendingAfter.response.body_sha256, "Parked addressing/grant/prompt bytes must survive reopen")
    const stateAfter = await request("parked.state-after-reopen", "GET", `/threads/${parkedThread}/state`)
    status(stateAfter, 200)
    for (const field of ["config", "parent_config", "metadata", "next", "values"])
      assert.equal(sha256(JSON.stringify(bodyOf(stateBefore)[field])), sha256(JSON.stringify(bodyOf(stateAfter)[field])), "Persisted checkpoint identity and values must survive graceful reopen")
    await request("parked.attach-after-reopen", "GET", `/threads/${parkedThread}/runs/stream`)
    status(await request("parked.delete", "DELETE", `/threads/${parkedThread}`), 204)
    status(await request("parked.deleted-state", "GET", `/threads/${parkedThread}/state`), 404)
    status(await request("parked.deleted-pending", "GET", `/threads/${parkedThread}/pending_interrupts`), 404)
    status(await request("parked.deleted-attach", "GET", `/threads/${parkedThread}/runs/stream`), 404)
    assert.equal((await ledger()).length, 0, "Parked tool must not execute")
    assertions.push("genuine_permission_checkpoint_grant_restored_then_deleted_without_tool_execution")
    assertions.push("parked_checkpoint_config_parent_provenance_next_and_values_remain_byte_equivalent_after_reopen")

    const liveThread = "t-lifecycle-live"
    const live = await begin("live.producer", "POST", `/threads/${liveThread}/runs/stream`, held("live"))
    await entered("live")
    status(await request("live.thread", "GET", `/threads/${liveThread}`), 200)
    status(await request("live.state", "GET", `/threads/${liveThread}/state`), 404)
    status(await request("live.delete-active", "DELETE", `/threads/${liveThread}`), 409)
    for (const [name, method, path, expected] of [["delete", "DELETE", `/threads/${liveThread}`, 403], ["cancel", "POST", `/threads/${liveThread}/cancel`, 403], ["state", "GET", `/threads/${liveThread}/state`, 404], ["attach", "GET", `/threads/${liveThread}/runs/stream`, 404]])
      status(await request(`live.foreign-${name}`, method, path, undefined, { caller: "other" }), expected)
    const liveAttach = await begin("live.attach", "GET", `/threads/${liveThread}/runs/stream`)
    await bounded(liveAttach.firstState, "live snapshot")
    await release("live")
    assert.equal(outputOf(await live.completed)?.key, "live")
    const tail = await liveAttach.completed
    assert.equal(JSON.stringify(events(tail)), JSON.stringify(["state", "done"]))
    assert.equal(tail.response.sse_frames[0].data.live, true)
    assert.equal(JSON.stringify(tail.response.sse_frames[0].data.turn), "[]")
    assert.equal(outputOf(tail)?.key, "live")
    await drained("live")
    status(await request("live.delete-completed", "DELETE", `/threads/${liveThread}`), 204)
    assertions.push("live_attachment_captures_exact_empty_workflow_digest_then_actual_terminal_tail")

    const waitThread = "t-lifecycle-wait-cancel"
    const wait = await begin("wait-cancel.producer", "POST", `/threads/${waitThread}/runs/wait`, held("wait-cancel"))
    await entered("wait-cancel")
    status(await request("wait-cancel.cancel", "POST", `/threads/${waitThread}/cancel`), 200)
    status(await wait.completed, 409)
    status(await request("wait-cancel.delete-before-quiescence", "DELETE", `/threads/${waitThread}`), 409)
    status(await request("wait-cancel.run-before-quiescence", "POST", `/threads/${waitThread}/runs/wait`, immediate("too-early")), 409)
    await release("wait-cancel"); await drained("wait-cancel")
    status(await request("wait-cancel.run-after-quiescence", "POST", `/threads/${waitThread}/runs/wait`, immediate("after-drain")), 200)
    status(await request("wait-cancel.delete-after-quiescence", "DELETE", `/threads/${waitThread}`), 204)
    assertions.push("wait_cancel_acknowledges_before_effect_quiescence_slot_and_delete_remain_fenced")

    const streamThread = "t-lifecycle-stream-cancel"
    const stream = await begin("stream-cancel.producer", "POST", `/threads/${streamThread}/runs/stream`, held("stream-cancel"))
    await entered("stream-cancel")
    const streamAttach = await begin("stream-cancel.attach", "GET", `/threads/${streamThread}/runs/stream`)
    await bounded(streamAttach.firstState, "cancel attachment snapshot")
    status(await request("stream-cancel.cancel", "POST", `/threads/${streamThread}/cancel`), 200)
    assert.equal(outputOf(await stream.completed)?.cancelled, true)
    assert.equal(outputOf(await streamAttach.completed)?.cancelled, true)
    status(await request("stream-cancel.delete-before-quiescence", "DELETE", `/threads/${streamThread}`), 409)
    await release("stream-cancel"); await drained("stream-cancel")
    status(await request("stream-cancel.run-after-quiescence", "POST", `/threads/${streamThread}/runs/wait`, immediate("after-drain")), 200)
    status(await request("stream-cancel.delete-after-quiescence", "DELETE", `/threads/${streamThread}`), 204)
    assertions.push("stream_cancel_primary_and_live_viewer_share_terminal_before_effect_quiescence")

    const capacityThread = "t-lifecycle-capacity"
    const capacity = await begin("capacity.producer", "POST", `/threads/${capacityThread}/runs/stream`, held("capacity"))
    await entered("capacity")
    const viewer = await begin("capacity.viewer-abort", "GET", `/threads/${capacityThread}/runs/stream`, undefined, { viewerAbort: true })
    await bounded(viewer.firstState, "capacity viewer snapshot")
    const refused = await request("capacity.viewer-refused", "GET", `/threads/${capacityThread}/runs/stream`)
    assert.equal(JSON.stringify(events(refused)), JSON.stringify(["detached"]))
    assert.equal(refused.response.sse_frames[0].data.reason, "capacity")
    viewer.abort()
    const prefix = await viewer.completed
    assert.equal(JSON.stringify(events(prefix)), JSON.stringify(["state"]))
    assert.equal(prefix.response.capture.complete, false)
    const busy = await request("capacity.thread-after-viewer-abort", "GET", `/threads/${capacityThread}`)
    assert.equal(bodyOf(busy).status, "busy")
    status(await request("capacity.run-after-viewer-abort", "POST", `/threads/${capacityThread}/runs/wait`, immediate("still-active")), 409)
    await release("capacity")
    assert.equal(outputOf(await capacity.completed)?.key, "capacity")
    await drained("capacity")
    await request("capacity.attach-after-completion", "GET", `/threads/${capacityThread}/runs/stream`)
    status(await request("capacity.delete", "DELETE", `/threads/${capacityThread}`), 204)
    assertions.push("real_viewer_capacity_detaches_one_viewer_aborted_prefix_leaves_producer_active")

    const raceThread = "t-lifecycle-late-cancel"
    const first = await begin("late-cancel.first-producer", "POST", `/threads/${raceThread}/runs/stream`, held("race-first"))
    await entered("race-first")
    let releasePolicy
    let signalPolicyEntered
    const policyEntered = new Promise(resolve => { signalPolicyEntered = resolve })
    policyHold = { entered: signalPolicyEntered, promise: new Promise(resolve => { releasePolicy = resolve }), release: () => releasePolicy() }
    const cancel = await begin("late-cancel.held-cancel", "POST", `/threads/${raceThread}/cancel`, undefined, { holdPolicy: true })
    await bounded(policyEntered, "late cancel policy entry")
    await release("race-first")
    assert.equal(outputOf(await first.completed)?.key, "race-first")
    await drained("race-first")
    const successor = await begin("late-cancel.successor-producer", "POST", `/threads/${raceThread}/runs/stream`, held("race-second"))
    await entered("race-second")
    releasePolicy(); policyHold = undefined
    status(await cancel.completed, 409)
    const successorThread = await request("late-cancel.successor-thread", "GET", `/threads/${raceThread}`)
    assert.equal(bodyOf(successorThread).status, "busy")
    const successorAttach = await begin("late-cancel.successor-attach", "GET", `/threads/${raceThread}/runs/stream`)
    await bounded(successorAttach.firstState, "successor snapshot")
    await release("race-second")
    assert.equal(outputOf(await successor.completed)?.key, "race-second")
    assert.equal(outputOf(await successorAttach.completed)?.key, "race-second")
    await drained("race-second")
    status(await request("late-cancel.delete", "DELETE", `/threads/${raceThread}`), 204)
    assertions.push("actual_async_policy_late_cancel_claim_cannot_abort_successor_turn")

    await handler.close(); handler = undefined
    profile = "ungated"
    ungatedHandler = await createRuntimeFetchHandler({ appRoot: ungatedRoot, drainDeadlineMs: 1_000, apSseHeartbeatIntervalMs: 60_000 })
    await absence("ungated-missing", { ungated: true })
    await ungatedHandler.close(); ungatedHandler = undefined
    assertions.push("ungated_missing_lifecycle_observations_are_separate_from_owner_policy_profile")
    assert.ok((await ledger()).every(entry => entry.phase !== "approved-tool"), "Parked decision never executes")
    for (const key of heldKeys) assert.equal((await ledger()).filter(entry => entry.key === key).length, 2, "Each held effect enters and quiesces exactly once")
  } finally {
    for (const controller of controllers) controller.abort(new Error("Fixture cleanup"))
    for (const key of heldKeys) await release(key)
    if (policyHold) policyHold.release()
    await Promise.allSettled(collectors)
    try { await bounded(Promise.all([handler?.close(), ungatedHandler?.close()]), "handler cleanup", 2_000) }
    finally {
      globalThis.fetch = originalFetch
      for (const [name, value] of previousEnvironment) { if (value === undefined) delete process.env[name]; else process.env[name] = value }
      await rm(temporaryRoot, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 })
    }
  }
  assert.equal(externalFetchAttempts, 0)
  assert.equal((await collectRuntimeArtifacts(referenceRoot)).inventory_sha256, artifacts.inventory_sha256, "Built runtime changed during capture")
  assert.equal(JSON.stringify(await verifyReferenceRoot(referenceRoot)), JSON.stringify(reference), "Pinned sources/lock changed")
  assert.equal((await exec("git", ["-C", referenceRoot, "status", "--porcelain"])).stdout, "", "Pinned source checkout changed")
  const recording = {
    format_version: 1, evidence_classification: "runtime_observation", transport: "in_process_fetch",
    protocol_surface: "Selected Agent Protocol cancel/delete/state/attach paths, actual Fetch handler; no TCP/full-suite parity",
    reference, runtime_entry: { path: runtimeEntry, sha256: sha256(runtimeBytes) },
    runtime_artifacts: { sidecar_file: "runtime-artifacts.json", inventory_sha256: artifacts.inventory_sha256, runtime_core: artifacts.runtime_core },
    reused_build_provenance: { file: "conformance/reference/recordings/2026-10-01-grants/build-provenance.json", path_base: "repository_root", sha256: sha256(reusedBuild), claim: "Reused unchanged freshly compiled scoped 13/13 zero-cache build; this capture performs no rebuild." },
    execution: { node_version: process.version, platform: process.platform, arch: process.arch, external_fetch_attempts: externalFetchAttempts, provider_calls: 0, deterministic_model_calls: modelCalls, imported_model_packages: importedModelPackages, fresh_databases: 2 },
    effective_dependency_versions: await dependencyVersions(referenceRoot),
    fixture_application: { revision: fixtureRevision, files: fixtureFiles, routes: ["/immediate#workflow", "/held#workflow", "/park#agent"], held_effect: "Local append-only ledger and release file; deliberately ignores abort signal, deadline 15 seconds; every effect drained before cleanup", policy: "owner header, server create stamp, owner allowed when row absent; foreign actors denied; held asynchronous thread.cancel policy is genuine handler option", ungated_profile: "Separate fresh app/database without threadAccess" },
    normalization: { dynamic: "Only generated top-level thread IDs/timestamps, known checkpoint IDs/parents, serialized HumanMessage IDs, actual permission interrupt/grant/resume keys, run_started_at, and retry jitter. Raw bytes, complete/cut marker, live snapshot cut, arrays, application input/output, event order, terminal outcomes and ledger counts are preserved.", scope: "Response facts and cut-time ledger projections only; no whole-runtime equivalence claim." },
    assertions, observations,
  }
  await mkdir(output, { recursive: true })
  const destination = join(output, "lifecycle-observations.json")
  const bytes = Buffer.from(`${JSON.stringify(recording, null, 2)}\n`)
  await writeFile(destination, bytes, { flag: "wx" })
  artifacts.recording_links = [{ path: "lifecycle-observations.json", path_base: "sidecar_directory", sha256: sha256(bytes), runtime_wrapper_sha256: recording.runtime_entry.sha256 }]
  await writeFile(join(output, "runtime-artifacts.json"), `${JSON.stringify(artifacts, null, 2)}\n`, { flag: "wx" })
  return { path: destination, sha256: sha256(bytes), cases: observations.length, fixture_revision: fixtureRevision, assertions: assertions.length }
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url))
  capture(parseArguments(process.argv.slice(2))).then(result => process.stdout.write(`${JSON.stringify(result, null, 2)}\n`)).catch(error => { process.stderr.write(`${error.stack ?? error}\n`); process.exitCode = 1 })
