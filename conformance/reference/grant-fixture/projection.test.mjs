import assert from "node:assert/strict"
import test from "node:test"
import { summarizeGrantObservation } from "./projection.mjs"

test("error projection retains exact runtime refusal and local execution count", () => {
  const response = { status: 403, body_utf8: JSON.stringify({ error: { message: "Exact refusal", details: { code: "grant_invalid" } } }) }
  assert.deepEqual(summarizeGrantObservation({ id: "invalid", response, ledger: [] }), {
    id: "invalid", status: 403, error: { message: "Exact refusal", code: "grant_invalid" }, ledger: [],
  })
})

test("pending projection omits opaque values but preserves prompt, shape and pair equality", () => {
  const response = { status: 200, body_utf8: JSON.stringify({ interrupts: [{ interruptId: "perm-generated", resumeKey: "outer-generated", grant: "b4ag_generated", value: { interruptId: "perm-generated", grant: "b4ag_generated", type: "permission-request", kind: "tool", detail: { toolName: "deployProd", argsPreview: '{"env":"staging"}', suggestedPattern: "deployProd" } } }] }) }
  assert.deepEqual(summarizeGrantObservation({ id: "pending", response, ledger: [] }), {
    id: "pending", status: 200, pending: [{ has_interrupt_id: true, has_resume_key: true, grant_prefix: "b4ag_", grant_bytes: 14, envelope_matches: true, prompt: { type: "permission-request", kind: "tool", detail: { toolName: "deployProd", argsPreview: '{"env":"staging"}', suggestedPattern: "deployProd" } } }], ledger: [],
  })
})
