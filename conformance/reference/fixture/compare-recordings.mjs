import assert from "node:assert/strict"
import { readFile, writeFile } from "node:fs/promises"
import { dirname, relative, resolve } from "node:path"
import { fileURLToPath } from "node:url"
import { normalizeResponseBody, parseSse, sha256 } from "./harness.mjs"

const scriptPath = fileURLToPath(import.meta.url)
const repositoryRoot = resolve(dirname(scriptPath), "../../..")
export const PROJECTION_FORMULA = 'recording.observations.map(o => ({id:o.id,request:o.request,response:projectResponse(o.response),...(o.thread_after ? {thread_after:projectResponse(o.thread_after)} : {})})); projectResponse(r) = ({status:r.status,status_text:r.status_text,headers:r.headers,body:r.normalized_body_utf8,...(r.sse_frames ? {sse_frames:r.sse_frames} : {})}); digest = SHA256(UTF8(JSON.stringify(projection)))'

function projectResponse(response) {
  return {
    status: response.status,
    status_text: response.status_text,
    headers: response.headers,
    body: response.normalized_body_utf8,
    ...(response.sse_frames ? { sse_frames: response.sse_frames } : {}),
  }
}

export function projectRecording(recording) {
  return recording.observations.map(observation => ({
    id: observation.id,
    request: observation.request,
    response: projectResponse(observation.response),
    ...(observation.thread_after ? { thread_after: projectResponse(observation.thread_after) } : {}),
  }))
}

function verifyBody(body) {
  const bytes = Buffer.from(body.body_base64, "base64")
  assert.ok(bytes.toString("base64") === body.body_base64 && bytes.toString("utf8") === body.body_utf8 && sha256(bytes) === body.body_sha256,
    "Raw body integrity differs from recorded base64, UTF-8 or SHA-256")
}

function verifyResponse(response, projection) {
  verifyBody(response)
  assert.equal(response.normalized_body_utf8, normalizeResponseBody(response.body_utf8, projection), "Normalized body differs from its raw response")
  if (response.headers.some(([key, value]) => key === "content-type" && value.includes("text/event-stream")))
    assert.deepEqual(response.sse_frames, parseSse(response.body_utf8), "SSE frames differ from raw body")
}

export function compareRecordingData(first, second) {
  for (const key of ["reference", "fixture_application", "runtime_entry", "effective_dependency_versions", "transport"])
    assert.deepEqual(first[key], second[key], `${key} differs between recordings`)
  for (const recording of [first, second]) {
    assert.equal(new Set(recording.observations.map(o => o.id)).size, recording.observations.length, "Case inventory differs due to duplicate identifiers")
    for (const observation of recording.observations) {
      verifyBody(observation.request)
      verifyResponse(observation.response, observation.id.startsWith("create-") ? "thread" : "none")
      if (observation.thread_after) verifyResponse(observation.thread_after, "thread")
    }
  }
  const firstProjection = projectRecording(first)
  const secondProjection = projectRecording(second)
  assert.deepEqual(firstProjection, secondProjection, "Recording projections differ")
  return {
    compared_cases: firstProjection.length,
    matching_cases: firstProjection.length,
    normalized_projection_sha256: sha256(JSON.stringify(firstProjection)),
  }
}

export async function compareRecordings({ firstPath, secondPath, output }) {
  if ([firstPath, secondPath].some(path => resolve(path) === resolve(output)))
    throw new Error("Comparison receipt cannot overwrite an input recording")
  const firstBytes = await readFile(firstPath)
  const secondBytes = await readFile(secondPath)
  const first = JSON.parse(firstBytes.toString("utf8"))
  const second = JSON.parse(secondBytes.toString("utf8"))
  const result = compareRecordingData(first, second)
  const receipt = {
    format_version: 2,
    evidence_classification: "runtime_repeatability_verification",
    reference_commit: first.reference.commit,
    fixture_revision: first.fixture_application.revision,
    transport: first.transport,
    first_recording_path: relative(repositoryRoot, resolve(firstPath)),
    first_recording_sha256: sha256(firstBytes),
    second_recording_path: relative(repositoryRoot, resolve(secondPath)),
    second_recording_sha256: sha256(secondBytes),
    ...result,
    comparison_script: { path: relative(repositoryRoot, scriptPath), sha256: sha256(await readFile(scriptPath)) },
    comparison_dependencies: [{ path: "conformance/reference/fixture/harness.mjs", sha256: sha256(await readFile(new URL("./harness.mjs", import.meta.url))) }],
    projection_formula: PROJECTION_FORMULA,
    fields_compared: ["request", "response.status", "response.status_text", "response.headers", "response.normalized_body_utf8", "response.sse_frames", "thread_after.status", "thread_after.status_text", "thread_after.headers", "thread_after.normalized_body_utf8"],
    volatile_normalization: first.normalization,
  }
  await writeFile(output, `${JSON.stringify(receipt, null, 2)}\n`)
  return receipt
}

function parseArguments(args) {
  const result = {}
  for (let index = 0; index < args.length; index += 2) {
    const key = { "--first": "firstPath", "--second": "secondPath", "--output": "output" }[args[index]]
    if (!key || !args[index + 1]) throw new Error("Usage: compare-recordings.mjs --first PATH --second PATH --output PATH")
    result[key] = resolve(args[index + 1])
  }
  if (!result.firstPath || !result.secondPath || !result.output) throw new Error("--first, --second and --output are required")
  return result
}

if (process.argv[1] && resolve(process.argv[1]) === scriptPath) {
  compareRecordings(parseArguments(process.argv.slice(2)))
    .then(receipt => process.stdout.write(`${JSON.stringify(receipt, null, 2)}\n`))
    .catch(error => { process.stderr.write(`${error.stack ?? error}\n`); process.exitCode = 1 })
}
