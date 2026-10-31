import assert from "node:assert/strict"
import { test } from "node:test"

const capture = await import("./capture.mjs")

test("capture refuses an unpinned checkout before loading a runtime", async () => {
  assert.equal(typeof capture.verifyReferenceRoot, "function")
  await assert.rejects(
    capture.verifyReferenceRoot(new URL("../../..", import.meta.url).pathname),
    /Reference checkout must be at 8acac534ba6eb960b34de217b0da45b8405023af/,
  )
})

test("capture arguments require an explicit reference root and output directory", () => {
  assert.equal(typeof capture.parseArguments, "function")
  assert.throws(() => capture.parseArguments([]), /--reference-root/)
  assert.throws(() => capture.parseArguments(["--reference-root", "/tmp/reference"]), /--output/)
  assert.deepEqual(capture.parseArguments(["--reference-root", "/tmp/reference", "--output", "/tmp/observations"]), {
    referenceRoot: "/tmp/reference", output: "/tmp/observations",
  })
})
