import assert from "node:assert/strict"
import test from "node:test"
import { parseLifecycleSse, projectLifecycleObservation } from "./projection.mjs"

test("retry-only blocks retain their wire position and original jitter", () => {
  const blocks = parseLifecycleSse('event: state\ndata: {"live":false}\n\nretry: 2817\n\nevent: done\ndata: {"output":null}\n\n')
  assert.deepEqual(blocks.map(block => block.kind), ["event", "retry", "event"])
  assert.equal(blocks[1].retry_ms, 2817)
})
test("runtime times are normalized without masking application output or snapshot cut", () => {
  const observation = { id: "live", ledger: [{ phase: "entered", key: "work" }], response: {
    status: 200, capture: { complete: true, cut: "eof" },
    sse_blocks: [{ kind: "event", event: "state", data: { live: true, run_started_at: "2026-10-01T01:00:00Z", anchor: null, turn: [], input: { created_at: "application-value" } } }, { kind: "event", event: "done", data: { output: { run_started_at: "business-value" } } }],
  } }
  const projected = projectLifecycleObservation(observation)
  assert.equal(projected.blocks[0].data.run_started_at, "<runtime-timestamp>")
  assert.equal(projected.blocks[0].data.input.created_at, "application-value")
  assert.equal(projected.blocks[1].data.output.run_started_at, "business-value")
  assert.deepEqual(projected.blocks[0].data.turn, [])
  assert.deepEqual(projected.ledger, observation.ledger)
})
test("aborted prefixes remain explicitly incomplete and never gain a terminal", () => {
  const projected = projectLifecycleObservation({ id: "abort", ledger: [], response: {
    status: 200, capture: { complete: false, cut: "viewer_abort" },
    sse_blocks: [{ kind: "event", event: "state", data: { live: true, turn: [] } }],
  } })
  assert.deepEqual(projected.capture, { complete: false, cut: "viewer_abort" })
  assert.deepEqual(projected.blocks.map(block => block.event), ["state"])
})
test("retry normalization retains missing hints, array order, and terminal outcome", () => {
  const project = blocks => projectLifecycleObservation({ id: "probe", ledger: [], response: { status: 200, capture: { complete: true, cut: "eof" }, sse_blocks: blocks } })
  const blocks = [{ kind: "retry", retry_ms: 1999 }, { kind: "event", event: "done", data: { output: { cancelled: true, values: [2, 1] } } }]
  assert.equal(project(blocks).blocks[0].retry_ms, "<retry-jitter>")
  assert.notDeepEqual(project(blocks), project(blocks.slice(1)))
  assert.deepEqual(project(blocks).blocks[1].data.output, { cancelled: true, values: [2, 1] })
})
test("unexpected retry values and JSON application timestamps are not normalized", () => {
  const projected = projectLifecycleObservation({ id: "application", request: { method: "POST", path: "/threads/fixed/runs/wait" }, ledger: [], response: { status: 200, capture: { complete: true, cut: "eof" }, body_utf8: '{"created_at":"application-value","thread_id":"application-id"}' } })
  assert.deepEqual(projected.body, { created_at: "application-value", thread_id: "application-id" })
  const retry = projectLifecycleObservation({ id: "retry", ledger: [], response: { status: 200, capture: { complete: true, cut: "eof" }, sse_blocks: [{ kind: "retry", retry_ms: 9999 }] } })
  assert.equal(retry.blocks[0].retry_ms, 9999)
})
test("only known runtime checkpoint provenance and nested HumanMessage IDs are masked", () => {
  const messages = [{ lc: 1, type: "constructor", id: ["langchain_core", "messages", "HumanMessage"], kwargs: { id: "random-human", content: "fixture input" } }]
  const value = { config: { configurable: { checkpoint_id: "checkpoint-a" } }, created_at: "runtime-time", metadata: { "b4:checkpoint-routes": { checkpointId: "checkpoint-a", routes: ["/park#agent"] } }, values: { messages, __pregel_tasks: [{ node: "tools", args: { messages, lg_tool_call: { id: "fixture-call", args: { id: "business-id" } } } }] } }
  const result = projectLifecycleObservation({ id: "parked.state", request: { method: "GET", path: "/threads/fixed/state" }, ledger: [], response: { status: 200, capture: { complete: true, cut: "eof" }, body_utf8: JSON.stringify(value) } }).body
  assert.equal(result.metadata["b4:checkpoint-routes"].checkpointId, "<generated-checkpoint-id>")
  assert.equal(result.values.__pregel_tasks[0].args.messages[0].kwargs.id, "<generated-human-message-id>")
  assert.equal(result.values.__pregel_tasks[0].args.lg_tool_call.args.id, "business-id")
  assert.deepEqual(result.metadata["b4:checkpoint-routes"].routes, ["/park#agent"])
})
test("generated idle thread ID is masked inside endpoint diagnostics only", () => {
  const result = projectLifecycleObservation({ id: "idle.cancel", request: { method: "POST", path: "/threads/generated-123/cancel" }, ledger: [], response: { status: 409, capture: { complete: true, cut: "eof" }, body_utf8: '{"error":{"message":"No run in flight for thread \\"generated-123\\"","details":{"code":"no_run_in_flight"}},"application":"generated-123"}' } })
  assert.equal(result.body.error.message, 'No run in flight for thread "<generated-thread-id>"')
  assert.equal(result.body.application, "generated-123")
})
