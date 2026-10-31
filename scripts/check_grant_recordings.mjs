import assert from "node:assert/strict"
import { readFile, readdir } from "node:fs/promises"
import { dirname, join } from "node:path"
import { fileURLToPath } from "node:url"
import { parseSse, sha256 } from "../conformance/reference/fixture/harness.mjs"
import { summarizeGrantObservation } from "../conformance/reference/grant-fixture/projection.mjs"

const root = dirname(dirname(fileURLToPath(import.meta.url)))
const folder = join(root, "conformance/reference/recordings/2026-10-01-grants")
const pin = "8acac534ba6eb960b34de217b0da45b8405023af"
const lock = "46be0b081db024914c6da5d861c9416392abeea3bd44cbc10b6fba0840494ca0"
const orderedExceptions = ["pair.pending", "pair.pending-after-refusals"]
const persisted = value => JSON.parse(JSON.stringify(value))
const json = async path => JSON.parse(await readFile(path, "utf8"))

function bytes(value, label) {
  const raw = Buffer.from(value.body_base64, "base64")
  assert.equal(raw.toString("base64"), value.body_base64, `${label} bytes are not canonical base64`)
  assert.equal(raw.toString("utf8"), value.body_utf8, `${label} bytes do not match UTF-8`)
  assert.equal(sha256(raw), value.body_sha256, `${label} bytes do not match recorded digest`)
}

export function verifyGrantObservation(observation) {
  assert.equal(observation.evidence_classification, "runtime_observation")
  if (observation.request.body_base64 !== undefined) bytes(observation.request, "request")
  else {
    assert.equal(observation.request.method, "GET", "Only captured GET requests omit body bytes")
    assert.equal(observation.request.body_utf8, undefined)
    assert.equal(observation.request.body_sha256, undefined)
  }
  bytes(observation.response, "response")
  assert.equal(observation.response.normalized_body_utf8, observation.response.body_utf8,
    "Grant response normalization must preserve untouched raw bytes")
  const isSse = observation.response.headers.some(([name, value]) =>
    name.toLowerCase() === "content-type" && value.split(";")[0].trim() === "text/event-stream")
  if (isSse) {
    assert.ok(Array.isArray(observation.response.sse_frames), "SSE content type requires retained frames")
    assert.deepEqual(parseSse(observation.response.body_utf8), observation.response.sse_frames,
      "SSE frames differ from retained response bytes")
  } else {
    assert.equal(observation.response.sse_frames, undefined,
      "Non-SSE response must not claim retained SSE frames")
  }
  return persisted(summarizeGrantObservation(observation))
}

async function fixtureFiles(directory, prefix = "") {
  const result = []
  const entries = await readdir(join(directory, prefix), { withFileTypes: true })
  for (const entry of entries.sort((a, b) => a.name.localeCompare(b.name))) {
    const path = prefix ? `${prefix}/${entry.name}` : entry.name
    if (entry.isDirectory()) result.push(...await fixtureFiles(directory, path))
    else if (entry.isFile()) {
      const raw = await readFile(join(directory, path))
      result.push({ path, sha256: sha256(raw), bytes: raw.length })
    } else throw new Error("Grant fixture must contain only regular files and directories")
  }
  return result
}

export function verifyGrantInventory(recording, inventory, recordingBytes) {
  assert.deepEqual(inventory.reference, recording.reference)
  assert.equal(inventory.observation_timing, "capture_time_inventory")
  assert.equal(inventory.files.length, 385)
  assert.equal(new Set(inventory.files.map(item => item.path)).size, 385)
  assert.deepEqual(inventory.files.map(item => item.path),
    inventory.files.map(item => item.path).sort(), "Runtime inventory paths must retain ASCII order")
  assert.equal(inventory.inventory_sha256, sha256(JSON.stringify(inventory.files)))
  assert.equal(recording.runtime_artifacts.inventory_sha256, inventory.inventory_sha256)
  assert.equal(recording.runtime_artifacts.sidecar_file, "runtime-artifacts.json")
  assert.equal(recording.runtime_entry.path, "packages/cli/dist/lib/dev/runtime-fetch-handler.js")
  const wrapper = inventory.files.find(item => item.path === recording.runtime_entry.path)
  assert.ok(wrapper, "Recorded runtime wrapper must have an inventory entry")
  assert.equal(wrapper.sha256, recording.runtime_entry.sha256)
  assert.deepEqual(inventory.runtime_wrapper, wrapper, "Sidecar runtime wrapper must match its inventory entry")
  const core = inventory.files.find(item => item.path === "packages/cli/dist/lib/dev/runtime-fetch-core.js")
  assert.ok(core, "Recorded runtime core must have an inventory entry")
  assert.deepEqual(inventory.runtime_core, core, "Sidecar runtime core must match its inventory entry")
  assert.deepEqual(recording.runtime_artifacts.runtime_core, core,
    "Recorded runtime core must match its inventory entry")
  assert.deepEqual(inventory.recording_links, [{
    path: "grant-observations.json",
    path_base: "sidecar_directory",
    sha256: sha256(recordingBytes),
    runtime_wrapper_sha256: wrapper.sha256,
  }], "Sidecar recording link must bind its local retained bytes and runtime wrapper")
}

async function verifyCapture(directory, files) {
  const recordingBytes = await readFile(join(directory, "grant-observations.json"))
  const recording = JSON.parse(recordingBytes.toString("utf8"))
  const inventory = await json(join(directory, "runtime-artifacts.json"))
  assert.deepEqual(recording.reference, { commit: pin, lockfile_sha256: lock })
  assert.equal(recording.evidence_classification, "runtime_observation")
  assert.equal(recording.transport, "in_process_fetch")
  assert.equal(recording.execution.external_fetch_attempts, 0)
  assert.equal(recording.execution.provider_calls, 0)
  assert.equal(recording.observations.length, 39)
  assert.equal(recording.assertions.length, 9)
  assert.equal(new Set(recording.observations.map(item => item.id)).size, 39)
  assert.deepEqual(recording.fixture_application.fixture_files, files)
  assert.equal(recording.fixture_application.revision, sha256(JSON.stringify(files)))
  verifyGrantInventory(recording, inventory, recordingBytes)
  const projection = recording.observations.map(verifyGrantObservation)
  assert.deepEqual(projection, recording.comparison_projection, "Saved projection differs from raw observations")
  assert.equal(sha256(JSON.stringify(projection)), recording.comparison_projection_sha256)
  return { recording, inventory, projection }
}

function pendingAsSet(value) {
  const copy = structuredClone(value)
  if (copy.pending) copy.pending.sort((a, b) => JSON.stringify(a).localeCompare(JSON.stringify(b)))
  return copy
}

export async function verifyGrantRecordings() {
  const files = await fixtureFiles(join(root, "conformance/reference/grant-fixture"))
  const first = await verifyCapture(folder, files)
  const repeat = await verifyCapture(join(folder, "repeat"), files)
  assert.deepEqual(first.inventory.files, repeat.inventory.files)
  const receipt = await json(join(folder, "grant-repeatability.json"))
  assert.equal(receipt.reference.commit, pin)
  assert.equal(receipt.reference.lockfile_sha256, lock)
  assert.equal(receipt.fixture_revision, first.recording.fixture_application.revision)
  for (const capture of receipt.captures) {
    assert.ok(["grant-observations.json", "repeat/grant-observations.json"].includes(capture.path))
    assert.equal(sha256(await readFile(join(folder, capture.path))), capture.sha256)
  }
  assert.equal(receipt.captures.length, 2)
  const differences = []
  let matches = 0
  for (let index = 0; index < first.projection.length; index++) {
    const left = first.projection[index]
    const right = repeat.projection[index]
    assert.equal(left.id, right.id)
    if (JSON.stringify(left) === JSON.stringify(right)) matches++
    else differences.push(left.id)
    assert.deepEqual(orderedExceptions.includes(left.id) ? pendingAsSet(left) : left,
      orderedExceptions.includes(right.id) ? pendingAsSet(right) : right)
  }
  assert.deepEqual(differences, orderedExceptions)
  assert.equal(matches, receipt.ordered_projection_equal_cases)
  assert.equal(matches, 37)
  assert.equal(receipt.pending_set_projection_equal_cases, 39)
  assert.deepEqual(receipt.ordered_projection_differences.map(item => item.id), orderedExceptions)
  const provenance = await json(join(folder, "build-provenance.json"))
  assert.equal(provenance.pinned_commit, pin)
  assert.equal(provenance.lockfile_sha256, lock)
  assert.equal(provenance.tracked_sources_unchanged_before_and_after_capture, true)
  assert.equal(provenance.build.tasks_succeeded, 13)
  assert.equal(provenance.build.tasks_total, 13)
  assert.equal(provenance.build.cache_hits, 0)
  assert.equal(provenance.install.lifecycle_scripts, false)
  for (const operation of [provenance.install, provenance.build]) {
    assert.equal(operation.exit_code, 0)
    assert.equal(sha256(await readFile(join(folder, operation.log_file))), operation.log_sha256)
  }
  assert.equal(provenance.capture.recording_sha256, sha256(await readFile(join(folder, "grant-observations.json"))))
  assert.equal(provenance.capture.repeat_recording_sha256, sha256(await readFile(join(folder, "repeat/grant-observations.json"))))
  assert.equal(provenance.capture.runtime_inventory_sha256, first.inventory.inventory_sha256)
  return { cases_each: 39, ordered_matches: 37, pending_set_matches: 39, runtime_artifacts: 385 }
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const result = await verifyGrantRecordings()
  process.stdout.write(JSON.stringify(result) + "\n")
}
