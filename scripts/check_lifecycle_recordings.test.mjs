import assert from "node:assert/strict"
import test from "node:test"
import { readFile } from "node:fs/promises"
import { sha256 } from "../conformance/reference/fixture/harness.mjs"
import {
  verifyLifecycleInventory,
  verifyLifecycleObservation,
  verifyLifecycleRecordings,
  verifyLifecycleRepeatability,
} from "./check_lifecycle_recordings.mjs"

const folder = new URL("../conformance/reference/recordings/2026-10-01-lifecycle/", import.meta.url)
const recordingBytes = await readFile(new URL("lifecycle-observations.json", folder))
const recording = JSON.parse(recordingBytes.toString("utf8"))
const repeat = JSON.parse(await readFile(new URL("repeat/lifecycle-observations.json", folder), "utf8"))
const inventory = JSON.parse(await readFile(new URL("runtime-artifacts.json", folder), "utf8"))
const receipt = JSON.parse(await readFile(new URL("lifecycle-repeatability.json", folder), "utf8"))
const sse = () => structuredClone(recording.observations.find(item => item.response.sse_blocks))
const withResponseText = (observation, text) => {
  observation.response.body_utf8 = text
  observation.response.body_base64 = Buffer.from(text).toString("base64")
  observation.response.body_sha256 = sha256(text)
}

test("retained lifecycle evidence validates both captures without sorting projections", async () => {
  assert.deepEqual(await verifyLifecycleRecordings(), {
    cases_each: 77, assertions_each: 11, ordered_matches: 77,
    fixture_files: 10, runtime_artifacts: 385, incomplete_captures_each: 1,
  })
})

test("bodyless GET, DELETE and POST cancel consistently omit every byte field", () => {
  for (const method of ["GET", "DELETE", "POST"]) {
    const observation = recording.observations.find(item => item.request.method === method && item.request.body_base64 === undefined)
    assert.ok(observation)
    assert.doesNotThrow(() => verifyLifecycleObservation(observation))
  }
})

test("lifecycle evidence rejects partly omitted request byte metadata", () => {
  const observation = structuredClone(recording.observations.find(item => item.request.body_base64))
  delete observation.request.body_utf8
  assert.throws(() => verifyLifecycleObservation(observation), /request bytes/)
})

test("lifecycle evidence rejects an unexplained bodyless POST workflow request", () => {
  const observation = structuredClone(recording.observations.find(item => item.request.body_base64))
  for (const key of ["body_base64", "body_utf8", "body_sha256"]) delete observation.request[key]
  assert.throws(() => verifyLifecycleObservation(observation), /bodyless request/)
})

test("lifecycle evidence rejects noncanonical response base64", () => {
  const observation = structuredClone(recording.observations[0])
  observation.response.body_base64 += "\n"
  assert.throws(() => verifyLifecycleObservation(observation), /response bytes/)
})

test("lifecycle evidence rejects corrupted request and response digests", () => {
  const request = structuredClone(recording.observations.find(item => item.request.body_base64))
  request.request.body_sha256 = "0".repeat(64)
  assert.throws(() => verifyLifecycleObservation(request), /request bytes/)
  const response = structuredClone(recording.observations[0])
  response.response.body_sha256 = "0".repeat(64)
  assert.throws(() => verifyLifecycleObservation(response), /response bytes/)
})

test("lifecycle evidence rejects invalid UTF-8 even with a matching digest and replacement text", () => {
  const observation = structuredClone(recording.observations[0])
  const raw = Buffer.from([0xff])
  observation.response.body_base64 = raw.toString("base64")
  observation.response.body_utf8 = raw.toString("utf8")
  observation.response.body_sha256 = sha256(raw)
  assert.throws(() => verifyLifecycleObservation(observation), /UTF-8/)
})

test("lifecycle evidence parses declared JSON rather than accepting arbitrary matching bytes", () => {
  const observation = structuredClone(recording.observations.find(item => item.response.headers.some(([name, value]) => name === "content-type" && value === "application/json")))
  withResponseText(observation, "broken-json")
  assert.throws(() => verifyLifecycleObservation(observation), /JSON/)
})

test("lifecycle evidence requires SSE blocks and frames for the retained content type", () => {
  for (const key of ["sse_blocks", "sse_frames"]) {
    const observation = sse()
    delete observation.response[key]
    assert.throws(() => verifyLifecycleObservation(observation), /SSE/)
  }
})

test("lifecycle evidence reconstructs retry-only blocks independently of event frames", () => {
  const observation = sse()
  const retry = observation.response.sse_blocks.find(block => block.kind === "retry")
  assert.ok(retry)
  retry.retry_ms += 1
  assert.throws(() => verifyLifecycleObservation(observation), /SSE blocks/)
})

test("lifecycle evidence reconstructs comment blocks from raw bytes", () => {
  const observation = sse()
  withResponseText(observation, ": retained comment\n\n" + observation.response.body_utf8)
  assert.throws(() => verifyLifecycleObservation(observation), /SSE blocks/)
})

test("lifecycle evidence rejects altered event frames while raw blocks stay intact", () => {
  const observation = sse()
  observation.response.sse_frames[0].event = "fabricated"
  assert.throws(() => verifyLifecycleObservation(observation), /SSE frames/)
})

test("lifecycle evidence requires explicit complete EOF or the known incomplete viewer abort", () => {
  const original = recording.observations.find(item => !item.response.capture.complete)
  assert.equal(original.id, "capacity.viewer-abort")
  for (const value of [undefined, { complete: true, cut: "viewer_abort" }, { complete: false, cut: "eof" }]) {
    const observation = structuredClone(original)
    observation.response.capture = value
    assert.throws(() => verifyLifecycleObservation(observation), /capture/)
  }
  const observation = structuredClone(recording.observations[0])
  observation.response.capture = { complete: false, cut: "viewer_abort" }
  assert.throws(() => verifyLifecycleObservation(observation), /capture/)
})

test("lifecycle evidence refuses a projection that changes its retained cut-time ledger", () => {
  const observation = structuredClone(recording.observations.find(item => item.ledger.length))
  observation.projection.ledger = []
  assert.throws(() => verifyLifecycleObservation(observation), /projection/)
})

test("lifecycle inventory rejects escaped recording links and incorrect local digests", () => {
  for (const [key, value] of [["path", "../lifecycle-observations.json"], ["sha256", "0".repeat(64)], ["runtime_wrapper_sha256", "0".repeat(64)]]) {
    const changed = structuredClone(inventory)
    changed.recording_links[0][key] = value
    assert.throws(() => verifyLifecycleInventory(recording, changed, recordingBytes), /recording link/)
  }
})

test("lifecycle inventory binds duplicated runtime core facts to its listed artifact", () => {
  const changed = structuredClone(inventory)
  const changedRecording = structuredClone(recording)
  changed.runtime_core.sha256 = "0".repeat(64)
  changedRecording.runtime_artifacts.runtime_core.sha256 = "0".repeat(64)
  assert.throws(() => verifyLifecycleInventory(changedRecording, changed, recordingBytes), /runtime core/)
})

test("lifecycle repeatability rejects reordered observations without sorting them", () => {
  const changed = structuredClone(repeat)
  ;[changed.observations[0], changed.observations[1]] = [changed.observations[1], changed.observations[0]]
  assert.throws(() => verifyLifecycleRepeatability(recording, changed, receipt), /order/)
})

test("lifecycle repeatability recomputes every comparison digest from observations", () => {
  const changed = structuredClone(receipt)
  changed.comparisons[0].canonical_projection_sha256 = "0".repeat(64)
  assert.throws(() => verifyLifecycleRepeatability(recording, repeat, changed), /comparison digest/)
})
