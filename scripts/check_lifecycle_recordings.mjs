import assert from "node:assert/strict"
import { readFile, readdir } from "node:fs/promises"
import { dirname, join } from "node:path"
import { fileURLToPath } from "node:url"
import { sha256 } from "../conformance/reference/fixture/harness.mjs"
import { parseLifecycleSse, projectLifecycleObservation } from "../conformance/reference/lifecycle-fixture/projection.mjs"

const root = dirname(dirname(fileURLToPath(import.meta.url)))
const folder = join(root, "conformance/reference/recordings/2026-10-01-lifecycle")
const priorFolder = join(root, "conformance/reference/recordings/2026-10-01-grants")
const reference = {
  commit: "8acac534ba6eb960b34de217b0da45b8405023af",
  lockfile_sha256: "46be0b081db024914c6da5d861c9416392abeea3bd44cbc10b6fba0840494ca0",
}
const inventoryDigest = "71e2663947013ddf3dbac74abcc1465b1364ae946a33256a2807c8ec35163018"
const priorReceipt = "conformance/reference/recordings/2026-10-01-grants/build-provenance.json"
const persisted = value => JSON.parse(JSON.stringify(value))
const equal = (left, right, label) => assert.ok(JSON.stringify(left) === JSON.stringify(right), label)
const json = async path => JSON.parse(await readFile(path, "utf8"))
const ascii = (left, right) => left < right ? -1 : left > right ? 1 : 0

// Compare sensitive bodies through boolean assertions, so a validation error
// prints a fixed explanation rather than retained prompts or opaque grant bytes.
function bytes(value, label) {
  const keys = ["body_base64", "body_utf8", "body_sha256"]
  assert.ok(keys.every(key => typeof value[key] === "string"), `${label} bytes require complete metadata`)
  const raw = Buffer.from(value.body_base64, "base64")
  assert.ok(raw.toString("base64") === value.body_base64, `${label} bytes are not canonical base64`)
  assert.ok(raw.toString("utf8") === value.body_utf8, `${label} bytes differ from recorded UTF-8`)
  assert.ok(Buffer.from(value.body_utf8, "utf8").equals(raw), `${label} bytes are not valid UTF-8`)
  assert.ok(sha256(raw) === value.body_sha256, `${label} bytes differ from recorded digest`)
}
function contentType(headers) {
  assert.ok(Array.isArray(headers), "Retained headers must be an ordered array")
  assert.ok(headers.every(pair => Array.isArray(pair) && pair.length === 2 && pair.every(value => typeof value === "string")), "Retained header entries must be string pairs")
  const values = headers.filter(([name]) => name.toLowerCase() === "content-type")
  assert.ok(values.length <= 1, "Retained content type must be unambiguous")
  return values[0]?.[1].split(";")[0].trim().toLowerCase()
}
function declaredJson(text, label) {
  let valid = true
  try { JSON.parse(text) } catch { valid = false }
  assert.ok(valid, `${label} declared JSON is invalid`)
}

export function verifyLifecycleObservation(observation) {
  assert.equal(observation.evidence_classification, "runtime_observation")
  const request = observation.request
  const response = observation.response
  const requestType = contentType(request.headers)
  if (["body_base64", "body_utf8", "body_sha256"].some(key => request[key] !== undefined)) {
    bytes(request, "request")
    assert.equal(requestType, "application/json", "Captured request body must declare JSON")
    declaredJson(request.body_utf8, "request")
  } else {
    assert.ok(request.method === "GET" || request.method === "DELETE" ||
      (request.method === "POST" && /^\/threads\/[^/]+\/cancel$/.test(request.path)),
    "Unexplained bodyless request in lifecycle capture")
    assert.equal(requestType, undefined, "Bodyless request must omit JSON content type")
  }
  bytes(response, "response")
  const responseType = contentType(response.headers)
  const isSse = responseType === "text/event-stream"
  const expectedCapture = observation.id === "capacity.viewer-abort"
    ? { complete: false, cut: "viewer_abort" } : { complete: true, cut: "eof" }
  equal(response.capture, expectedCapture, "Required complete/cut capture metadata differs from the qualified observation")
  if (isSse) {
    assert.ok(Array.isArray(response.sse_blocks), "SSE content type requires retained wire blocks")
    assert.ok(Array.isArray(response.sse_frames), "SSE content type requires retained event frames")
    const blocks = parseLifecycleSse(response.body_utf8)
    equal(blocks, response.sse_blocks, "SSE blocks differ from retained raw response bytes")
    const frames = blocks.filter(block => block.kind === "event").map(({ kind: _, ...frame }) => frame)
    equal(frames, response.sse_frames, "SSE frames differ from retained raw event blocks")
    if (expectedCapture.complete) {
      assert.ok(/\r?\n\r?\n$/.test(response.body_utf8), "Complete SSE capture must end at a retained block boundary")
      const terminals = frames.filter(frame => ["done", "detached"].includes(frame.event))
      assert.equal(terminals.length, 1, "Complete qualified SSE capture must retain one terminal")
      assert.ok(frames.at(-1) === terminals[0], "Complete SSE terminal must preserve its final event position")
    } else {
      equal(frames.map(frame => frame.event), ["state"], "Incomplete viewer_abort capture must retain only its observed state prefix")
    }
  } else {
    assert.equal(response.sse_blocks, undefined, "Non-SSE response must not claim retained SSE blocks")
    assert.equal(response.sse_frames, undefined, "Non-SSE response must not claim retained SSE frames")
    assert.ok(expectedCapture.complete, "Only the qualified SSE viewer_abort capture is incomplete")
    if (responseType === "application/json") declaredJson(response.body_utf8, "response")
    else assert.ok(responseType === undefined && response.body_utf8 === "", "Qualified non-JSON response must retain its empty body")
  }
  assert.ok(Array.isArray(observation.ledger), "Cut-time ledger must be retained")
  assert.ok(observation.ledger.every(entry => ["entered", "exited"].includes(entry.phase) && typeof entry.key === "string"), "Cut-time ledger must retain known local effect phases")
  const projection = persisted(projectLifecycleObservation(observation))
  equal(projection, observation.projection, "Saved lifecycle projection differs from raw response and cut-time ledger")
  return projection
}

export function verifyLifecycleInventory(recording, inventory, recordingBytes) {
  assert.equal(inventory.format_version, 1)
  assert.equal(inventory.evidence_classification, "built_runtime_artifact_inventory")
  assert.deepEqual(inventory.reference, reference)
  assert.deepEqual(recording.reference, reference)
  assert.equal(inventory.observation_timing, "capture_time_inventory")
  assert.equal(inventory.files.length, 385)
  const paths = inventory.files.map(item => item.path)
  assert.equal(new Set(paths).size, 385)
  assert.deepEqual(paths, [...paths].sort(ascii), "Runtime inventory must retain ASCII path order")
  assert.ok(inventory.files.every(item => /^packages\/[^/]+\/dist\/.+\.(?:js|mjs|cjs|json)$/.test(item.path) && /^[0-9a-f]{64}$/.test(item.sha256) && Number.isSafeInteger(item.bytes) && item.bytes >= 0), "Runtime inventory entries require qualified paths, byte counts and digests")
  assert.equal(sha256(JSON.stringify(inventory.files)), inventory.inventory_sha256)
  assert.equal(inventory.inventory_sha256, inventoryDigest)
  assert.equal(recording.runtime_artifacts.inventory_sha256, inventoryDigest)
  assert.equal(recording.runtime_artifacts.sidecar_file, "runtime-artifacts.json")
  assert.equal(recording.runtime_entry.path, "packages/cli/dist/lib/dev/runtime-fetch-handler.js")
  const wrapper = inventory.files.find(item => item.path === recording.runtime_entry.path)
  assert.ok(wrapper, "Runtime wrapper must have an inventory entry")
  assert.equal(recording.runtime_entry.sha256, wrapper.sha256)
  assert.deepEqual(inventory.runtime_wrapper, wrapper, "Sidecar runtime wrapper must match its inventory entry")
  const core = inventory.files.find(item => item.path === "packages/cli/dist/lib/dev/runtime-fetch-core.js")
  assert.ok(core, "Runtime core must have an inventory entry")
  assert.deepEqual(inventory.runtime_core, core, "Sidecar runtime core must match its inventory entry")
  assert.deepEqual(recording.runtime_artifacts.runtime_core, core, "Recording runtime core must match its inventory entry")
  assert.deepEqual(inventory.recording_links, [{
    path: "lifecycle-observations.json", path_base: "sidecar_directory",
    sha256: sha256(recordingBytes), runtime_wrapper_sha256: wrapper.sha256,
  }], "Sidecar recording link must bind its local retained bytes and runtime wrapper")
}

async function fixtureFiles(directory, prefix = "") {
  const result = []
  const entries = await readdir(join(directory, prefix), { withFileTypes: true })
  for (const entry of entries.sort((left, right) => ascii(left.name, right.name))) {
    const path = prefix ? `${prefix}/${entry.name}` : entry.name
    if (entry.isDirectory()) result.push(...await fixtureFiles(directory, path))
    else {
      assert.ok(entry.isFile(), "Lifecycle fixture must contain only regular files and directories")
      const raw = await readFile(join(directory, path))
      result.push({ path, sha256: sha256(raw), bytes: raw.length })
    }
  }
  return result
}
async function verifyCapture(directory, files, reusedBuild) {
  const recordingBytes = await readFile(join(directory, "lifecycle-observations.json"))
  const recording = JSON.parse(recordingBytes.toString("utf8"))
  const inventory = await json(join(directory, "runtime-artifacts.json"))
  assert.equal(recording.format_version, 1)
  assert.equal(recording.evidence_classification, "runtime_observation")
  assert.equal(recording.transport, "in_process_fetch")
  assert.deepEqual(recording.reference, reference)
  assert.equal(recording.execution.external_fetch_attempts, 0)
  assert.equal(recording.execution.provider_calls, 0)
  assert.equal(recording.execution.deterministic_model_calls, 1)
  assert.equal(recording.execution.imported_model_packages, 1)
  assert.equal(recording.execution.fresh_databases, 2)
  assert.equal(recording.execution.node_version, "v24.20.0")
  assert.equal(recording.observations.length, 77)
  assert.equal(recording.assertions.length, 11)
  assert.equal(new Set(recording.assertions).size, 11)
  assert.ok(recording.assertions.every(item => typeof item === "string"), "Behavioral assertion names must be retained")
  assert.equal(new Set(recording.observations.map(item => item.id)).size, 77)
  assert.deepEqual(recording.observations.map(item => item.sequence), Array.from({ length: 77 }, (_, index) => index + 1), "Observation admission sequence must retain its original order")
  assert.equal(recording.observations.filter(item => !item.response.capture?.complete).length, 1)
  assert.equal(recording.observations.filter(item => item.profile === "owner_policy").length, 73)
  assert.equal(recording.observations.filter(item => item.profile === "ungated").length, 4)
  assert.deepEqual(recording.fixture_application.files, files)
  assert.equal(recording.fixture_application.revision, sha256(JSON.stringify(files)))
  assert.deepEqual(recording.fixture_application.routes, ["/immediate#workflow", "/held#workflow", "/park#agent"])
  assert.equal(recording.reused_build_provenance.file, priorReceipt)
  assert.equal(recording.reused_build_provenance.path_base, "repository_root")
  assert.equal(recording.reused_build_provenance.sha256, sha256(reusedBuild))
  verifyLifecycleInventory(recording, inventory, recordingBytes)
  const projection = recording.observations.map(verifyLifecycleObservation)
  for (const item of [inventory.inventory_script, ...inventory.inventory_dependencies]) {
    assert.ok(["conformance/reference/fixture/runtime-artifacts.mjs", "conformance/reference/fixture/harness.mjs", "conformance/reference/fixture/capture.mjs"].includes(item.path), "Inventory implementation path must remain repository-local and qualified")
    assert.equal(sha256(await readFile(join(root, item.path))), item.sha256)
  }
  assert.equal(inventory.inventory_dependencies.length, 2)
  assert.equal(new Set([inventory.inventory_script.path, ...inventory.inventory_dependencies.map(item => item.path)]).size, 3)
  return { recording, recordingBytes, inventory, projection }
}

export function verifyLifecycleRepeatability(first, repeat, receipt) {
  assert.equal(receipt.format_version, 1)
  assert.equal(receipt.evidence_classification, "limited_ordered_repeat_projection")
  assert.deepEqual(receipt.reference, reference)
  assert.equal(receipt.fixture_revision, first.fixture_application.revision)
  assert.equal(receipt.runtime_inventory_sha256, inventoryDigest)
  assert.equal(receipt.cases_each, 77)
  assert.equal(receipt.ordered_matches, 77)
  assert.deepEqual(receipt.mismatches, [])
  assert.equal(receipt.comparisons.length, 77)
  const left = first.observations.map(verifyLifecycleObservation)
  const right = repeat.observations.map(verifyLifecycleObservation)
  assert.equal(left.length, 77)
  assert.equal(right.length, 77)
  for (let index = 0; index < left.length; index++) {
    const a = left[index], b = right[index], comparison = receipt.comparisons[index]
    assert.equal(a.id, b.id, "Repeated lifecycle observations changed their retained order")
    assert.equal(a.id, comparison.id, "Repeat comparisons changed their retained order")
    equal(a, b, "Limited lifecycle projections differ in retained order")
    assert.equal(comparison.canonical_projection_sha256, sha256(JSON.stringify(a)), "Canonical comparison digest differs from raw-derived projection")
    assert.equal(comparison.repeat_projection_sha256, sha256(JSON.stringify(b)), "Repeat comparison digest differs from raw-derived projection")
    assert.equal(comparison.ordered_match, true)
  }
  assert.equal(receipt.canonical.file, "lifecycle-observations.json")
  assert.equal(receipt.repeat.file, "repeat/lifecycle-observations.json")
  assert.equal(receipt.canonical.projection_sha256, sha256(JSON.stringify(left)))
  assert.equal(receipt.repeat.projection_sha256, sha256(JSON.stringify(right)))
  assert.deepEqual(receipt.scope, first.normalization)
  assert.deepEqual(first.normalization, repeat.normalization)
  assert.equal(receipt.raw_bytes_identical, false)
  return { ordered_matches: 77 }
}

export async function verifyLifecycleRecordings() {
  const files = await fixtureFiles(join(root, "conformance/reference/lifecycle-fixture"))
  assert.equal(files.length, 10)
  const priorBytes = await readFile(join(root, priorReceipt))
  const prior = JSON.parse(priorBytes.toString("utf8"))
  const first = await verifyCapture(folder, files, priorBytes)
  const repeat = await verifyCapture(join(folder, "repeat"), files, priorBytes)
  const priorInventory = await json(join(priorFolder, "runtime-artifacts.json"))
  assert.deepEqual(first.inventory.files, repeat.inventory.files)
  assert.deepEqual(first.inventory.files, priorInventory.files)
  assert.equal(priorInventory.inventory_sha256, inventoryDigest)
  const receipt = await json(join(folder, "lifecycle-repeatability.json"))
  verifyLifecycleRepeatability(first.recording, repeat.recording, receipt)
  for (const [value, bytes] of [[receipt.canonical, first.recordingBytes], [receipt.repeat, repeat.recordingBytes]])
    assert.equal(value.sha256, sha256(bytes))
  assert.ok(!first.recordingBytes.equals(repeat.recordingBytes), "Independent raw captures must not be mistaken for identical bytes")
  const provenance = await json(join(folder, "build-provenance.json"))
  assert.equal(provenance.format_version, 1)
  assert.equal(provenance.pinned_commit, reference.commit)
  assert.equal(provenance.lockfile_sha256, reference.lockfile_sha256)
  assert.equal(provenance.node, "v24.20.0")
  assert.equal(provenance.source_state_before_and_after_both_captures, "clean tracked and untracked source status")
  assert.deepEqual(provenance.build_reuse.prior_provenance, {
    file: priorReceipt, path_base: "repository_root", sha256: sha256(priorBytes),
  })
  assert.equal(provenance.build_reuse.inventory_sha256, inventoryDigest)
  assert.equal(provenance.build_reuse.build_command, prior.build.command)
  for (const key of ["tasks_succeeded", "tasks_total", "cache_hits"])
    assert.equal(provenance.build_reuse[key], prior.build[key])
  assert.equal(prior.pinned_commit, reference.commit)
  assert.equal(prior.lockfile_sha256, reference.lockfile_sha256)
  assert.equal(prior.tracked_sources_unchanged_before_and_after_capture, true)
  assert.equal(prior.build.tasks_succeeded, 13)
  assert.equal(prior.build.tasks_total, 13)
  assert.equal(prior.build.cache_hits, 0)
  assert.equal(prior.install.lifecycle_scripts, false)
  assert.equal(prior.install.selected_workspace_projects, 12)
  assert.equal(prior.install.workspace_projects_total, 32)
  assert.ok(prior.install.command.includes("--frozen-lockfile") && prior.install.command.includes("--ignore-scripts"), "Reused installation must retain frozen lock and lifecycle-script exclusions")
  for (const operation of [prior.install, prior.build]) {
    assert.equal(operation.exit_code, 0)
    assert.ok(["install.log", "build.log"].includes(operation.log_file), "Prior build log path must remain qualified")
    assert.equal(sha256(await readFile(join(priorFolder, operation.log_file))), operation.log_sha256)
  }
  assert.equal(provenance.capture.transport, "in_process_fetch")
  assert.equal(provenance.capture.fresh_applications_and_databases_per_capture, 2)
  assert.equal(provenance.capture.requests_per_capture, 77)
  assert.equal(provenance.capture.assertions_per_capture, 11)
  assert.equal(provenance.capture.deterministic_model_calls_per_capture, 1)
  assert.equal(provenance.capture.provider_calls_per_capture, 0)
  assert.equal(provenance.capture.external_fetch_attempts_per_capture, 0)
  assert.equal(provenance.capture.fixture_revision, sha256(JSON.stringify(files)))
  for (const [name, capture] of [["canonical", first], ["repeat", repeat]]) {
    const metadata = provenance.capture[name]
    assert.equal(metadata.file, name === "canonical" ? "lifecycle-observations.json" : "repeat/lifecycle-observations.json")
    assert.equal(metadata.log_file, name === "canonical" ? "capture.log" : "repeat/capture.log")
    assert.equal(metadata.sha256, sha256(capture.recordingBytes))
    assert.equal(metadata.log_sha256, sha256(await readFile(join(folder, metadata.log_file))))
  }
  return {
    cases_each: 77, assertions_each: 11, ordered_matches: 77,
    fixture_files: 10, runtime_artifacts: 385, incomplete_captures_each: 1,
  }
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  try {
    process.stdout.write(JSON.stringify(await verifyLifecycleRecordings()) + "\n")
  } catch (error) {
    // Do not render assertion operands from sensitive raw-recording fields.
    process.stderr.write(`Lifecycle evidence validation failed: ${error.message}\n`)
    process.exitCode = 1
  }
}
