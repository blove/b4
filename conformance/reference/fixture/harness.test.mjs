import assert from "node:assert/strict"
import { readFile } from "node:fs/promises"
import { test } from "node:test"

const harness = await import("./harness.mjs")

test("normalization replaces only documented volatile response fields", () => {
  assert.equal(typeof harness.normalizeResponseBody, "function")
  const body = JSON.stringify({
    thread_id: "th-random",
    created_at: "2026-01-01T00:00:00.000Z",
    updated_at: "2026-01-01T00:00:01.000Z",
    metadata: { thread_id: "fixture data", created_at: "fixture data" },
    status: "idle",
  })
  assert.equal(harness.normalizeResponseBody(body, "thread"), JSON.stringify({
    thread_id: "<generated-thread-id>",
    created_at: "<generated-timestamp>",
    updated_at: "<generated-timestamp>",
    metadata: { thread_id: "fixture data", created_at: "fixture data" },
    status: "idle",
  }))
  assert.equal(harness.normalizeResponseBody(body, "none"), body)
  assert.equal(harness.normalizeResponseBody("null", "thread"), "null")
})

test("SSE decoding preserves terminal output and direct chunk data", () => {
  assert.equal(typeof harness.parseSse, "function")
  const source = ': heartbeat\r\nevent: chunk\r\ndata: {"text":"a"}\r\n\r\nevent: done\ndata: {}\n\n'
  assert.deepEqual(harness.parseSse(source), [
    { event: "chunk", data: { text: "a" }, data_text: '{"text":"a"}' },
    { event: "done", data: {}, data_text: "{}" },
  ])
  assert.deepEqual(harness.parseSse('event: done\ndata: {"output":null}\n\n'), [
    { event: "done", data: { output: null }, data_text: '{"output":null}' },
  ])
})

test("recording retains exact response bytes and their digest alongside normalization", async () => {
  assert.equal(typeof harness.recordResponse, "function")
  const response = new Response('{"thread_id":"abc","metadata":{"x":1}}', {
    headers: { "content-type": "application/json", "x-fixture": "retained" },
    status: 200,
  })
  const captured = await harness.recordResponse(response, "thread")
  assert.equal(captured.body_utf8, '{"thread_id":"abc","metadata":{"x":1}}')
  assert.equal(captured.body_base64, Buffer.from(captured.body_utf8).toString("base64"))
  assert.equal(captured.normalized_body_utf8, '{"thread_id":"<generated-thread-id>","metadata":{"x":1}}')
  assert.equal(captured.body_sha256, "dc92a3e1c5c9a6caaf3c012e5d18dd37b6e8bb494a9bb7bc660f25b9c9967131")
  assert.deepEqual(captured.headers, [["content-type", "application/json"], ["x-fixture", "retained"]])
})

test("independent request corpus covers parser, input and paired terminal routes", async () => {
  const cases = JSON.parse(await readFile(new URL("./cases.json", import.meta.url), "utf8"))
  assert.ok(cases.length >= 40)
  assert.equal(new Set(cases.map((entry) => entry.id)).size, cases.length)
  for (const id of ["create-empty", "create-whitespace", "create-array", "create-duplicate-metadata", "create-wrong-content-type", "wait-input-omitted", "wait-input-null", "wait-input-string", "wait-input-array", "wait-unknown-route"])
    assert.ok(cases.some((entry) => entry.id === id), id)
  for (const route of ["void", "zero", "false", "empty", "fail", "validate"])
    for (const mode of ["wait", "stream"])
      assert.ok(cases.some((entry) => entry.id === `${mode}-${route}`), `${mode}-${route}`)
  for (const entry of cases) {
    assert.equal(typeof entry.body, "string", entry.id)
    assert.equal(entry.method, "POST", entry.id)
    assert.ok(["CP-001", "CP-002", "CP-003", "CP-004"].includes(entry.characterization), entry.id)
  }
})
