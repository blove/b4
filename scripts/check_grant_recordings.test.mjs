import assert from "node:assert/strict"
import test from "node:test"
import { readFile } from "node:fs/promises"
import { verifyGrantInventory, verifyGrantObservation, verifyGrantRecordings } from "./check_grant_recordings.mjs"

const recording = JSON.parse(await readFile(new URL("../conformance/reference/recordings/2026-10-01-grants/grant-observations.json", import.meta.url), "utf8"))

test("retained grant evidence validates with explicitly limited repeatability", async () => {
  const result = await verifyGrantRecordings()
  assert.equal(result.cases_each, 39)
  assert.equal(result.ordered_matches, 37)
  assert.equal(result.pending_set_matches, 39)
})

test("grant evidence rejects response byte corruption before comparing summaries", () => {
  const observation = structuredClone(recording.observations[0])
  observation.response.body_base64 = Buffer.from("corrupt").toString("base64")
  assert.throws(() => verifyGrantObservation(observation), /response bytes/)
})

test("grant evidence rejects altered request identity bytes", () => {
  const observation = structuredClone(recording.observations[0])
  observation.request.body_sha256 = "0".repeat(64)
  assert.throws(() => verifyGrantObservation(observation), /request bytes/)
})

test("grant evidence reconstructs SSE frames from the retained response", () => {
  const observation = structuredClone(recording.observations.find(item => item.response.sse_frames))
  observation.response.sse_frames = [{ event: "done", data: {}, data_text: "{}" }]
  assert.throws(() => verifyGrantObservation(observation), /SSE/)
})

test("grant evidence requires SSE frames when retained content type declares SSE", () => {
  const observation = structuredClone(recording.observations.find(item => item.response.sse_frames))
  delete observation.response.sse_frames
  assert.throws(() => verifyGrantObservation(observation), /SSE/)
})

test("grant evidence rejects normalization of its untouched raw response", () => {
  const observation = structuredClone(recording.observations[0])
  observation.response.normalized_body_utf8 = "altered comparison text"
  assert.throws(() => verifyGrantObservation(observation), /normaliz/)
})

const recordingBytes = await readFile(new URL("../conformance/reference/recordings/2026-10-01-grants/grant-observations.json", import.meta.url))
const inventory = JSON.parse(await readFile(new URL("../conformance/reference/recordings/2026-10-01-grants/runtime-artifacts.json", import.meta.url), "utf8"))

test("grant inventory rejects an escaped or incorrect recording link", () => {
  const changed = structuredClone(inventory)
  changed.recording_links[0].path = "../grant-observations.json"
  assert.throws(() => verifyGrantInventory(recording, changed, recordingBytes), /recording link/)
})

test("grant inventory rejects a recording link digest unrelated to retained bytes", () => {
  const changed = structuredClone(inventory)
  changed.recording_links[0].sha256 = "0".repeat(64)
  assert.throws(() => verifyGrantInventory(recording, changed, recordingBytes), /recording link/)
})

test("grant inventory rejects a recording link naming a different runtime wrapper", () => {
  const changed = structuredClone(inventory)
  changed.recording_links[0].runtime_wrapper_sha256 = "0".repeat(64)
  assert.throws(() => verifyGrantInventory(recording, changed, recordingBytes), /recording link/)
})

test("grant inventory ties core metadata to the listed artifact rather than its duplicate", () => {
  const changed = structuredClone(inventory)
  const changedRecording = structuredClone(recording)
  changed.runtime_core.sha256 = "0".repeat(64)
  changedRecording.runtime_artifacts.runtime_core.sha256 = "0".repeat(64)
  assert.throws(() => verifyGrantInventory(changedRecording, changed, recordingBytes), /runtime core/)
})
