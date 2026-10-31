import assert from "node:assert/strict"
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { test } from "node:test"
import { normalizeResponseBody, sha256 } from "./harness.mjs"

const comparison = await import("./compare-recordings.mjs")

function response(body, normalization = "none") {
  return { status: 200, status_text: "", headers: [["content-type", "application/json"]], body_utf8: body,
    body_base64: Buffer.from(body).toString("base64"), body_sha256: sha256(body), normalized_body_utf8: normalizeResponseBody(body, normalization) }
}

function recording(threadId = "first", timestamp = "first-time") {
  return { reference: { commit: "pinned", lockfile_sha256: "lock" }, fixture_application: { revision: "fixture" },
    runtime_entry: { path: "wrapper", sha256: "wrapper-hash" }, effective_dependency_versions: { sdk: "1" }, transport: "in_process_fetch",
    observations: [{ id: "create-object", request: { method: "POST", path: "/threads", headers: [], body_utf8: "{}", body_base64: "e30=", body_sha256: sha256("{}") },
      response: response(JSON.stringify({ thread_id: threadId, created_at: timestamp, updated_at: timestamp, metadata: { retained: "app-data" }, status: "idle" }), "thread") }] }
}

test("comparison ignores documented thread volatility while preserving raw body integrity", () => {
  assert.equal(typeof comparison.compareRecordingData, "function")
  const result = comparison.compareRecordingData(recording(), recording("second", "second-time"))
  assert.equal(result.matching_cases, 1)
  assert.equal(result.compared_cases, 1)
  assert.equal(result.normalized_projection_sha256.length, 64)
})

test("comparison rejects application payload, header and case inventory differences", () => {
  assert.equal(typeof comparison.compareRecordingData, "function")
  for (const field of ["payload", "header", "inventory"]) {
    const second = recording()
    if (field === "payload") second.observations[0].response = response('{"metadata":{"retained":"changed"}}', "thread")
    if (field === "header") second.observations[0].response.headers[0][1] = "text/plain"
    if (field === "inventory") second.observations[0].id = "different-case"
    assert.throws(() => comparison.compareRecordingData(recording(), second), /differ/, field)
  }
})

test("comparison rejects corrupt raw byte evidence even when normalized bodies agree", () => {
  assert.equal(typeof comparison.compareRecordingData, "function")
  const second = recording()
  second.observations[0].response.body_base64 = Buffer.from("corrupt").toString("base64")
  assert.throws(() => comparison.compareRecordingData(recording(), second), /Raw body integrity/)
})

test("projection has an exact independently reproducible JSON formula", () => {
  assert.equal(typeof comparison.projectRecording, "function")
  const input = recording()
  const selected = input.observations[0]
  const expected = [{ id: selected.id, request: selected.request, response: {
    status: selected.response.status, status_text: selected.response.status_text, headers: selected.response.headers, body: selected.response.normalized_body_utf8,
  } }]
  assert.equal(JSON.stringify(comparison.projectRecording(input)), JSON.stringify(expected))
})

test("comparison cannot overwrite an input recording with its receipt", async () => {
  const root = await mkdtemp(join(tmpdir(), "b4-comparison-input-test-"))
  try {
    const firstPath = join(root, "first.json")
    const secondPath = join(root, "second.json")
    const original = JSON.stringify(recording())
    await writeFile(firstPath, original)
    await writeFile(secondPath, original)
    await assert.rejects(comparison.compareRecordings({ firstPath, secondPath, output: firstPath }), /overwrite an input recording/)
    assert.equal(await readFile(firstPath, "utf8"), original)
  } finally { await rm(root, { recursive: true, force: true }) }
})
