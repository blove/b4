import assert from "node:assert/strict"
import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { test } from "node:test"
import { sha256 } from "./harness.mjs"

const provenance = await import("./runtime-artifacts.mjs")

test("runtime inventory includes wrapper, core and all workspace dist JS/JSON in stable order", async () => {
  assert.equal(typeof provenance.collectRuntimeArtifacts, "function")
  const root = await mkdtemp(join(tmpdir(), "b4-runtime-artifact-test-"))
  try {
    for (const [path, contents] of [
      ["packages/cli/dist/lib/dev/runtime-fetch-handler.js", "wrapper"], ["packages/cli/dist/lib/dev/runtime-fetch-core.js", "core"],
      ["packages/sdk/dist/index.js", "sdk"], ["packages/sdk/dist/data.json", "{}"],
      ["packages/sdk/dist/index.d.ts", "type ignored"], ["packages/sdk/src/index.ts", "source ignored"],
    ]) {
      await mkdir(join(root, path, ".."), { recursive: true })
      await writeFile(join(root, path), contents)
    }
    const result = await provenance.collectRuntimeArtifacts(root)
    assert.deepEqual(result.files.map(file => file.path), ["packages/cli/dist/lib/dev/runtime-fetch-core.js", "packages/cli/dist/lib/dev/runtime-fetch-handler.js", "packages/sdk/dist/data.json", "packages/sdk/dist/index.js"])
    assert.equal(result.runtime_wrapper.sha256, sha256("wrapper"))
    assert.equal(result.runtime_core.sha256, sha256("core"))
    assert.equal(result.inventory_sha256, sha256(JSON.stringify(result.files)))
  } finally { await rm(root, { recursive: true, force: true }) }
})

test("runtime inventory refuses to qualify a missing core even when the wrapper exists", async () => {
  assert.equal(typeof provenance.collectRuntimeArtifacts, "function")
  const root = await mkdtemp(join(tmpdir(), "b4-runtime-artifact-test-"))
  try {
    await mkdir(join(root, "packages/cli/dist/lib/dev"), { recursive: true })
    await writeFile(join(root, "packages/cli/dist/lib/dev/runtime-fetch-handler.js"), "wrapper")
    await assert.rejects(provenance.collectRuntimeArtifacts(root), /runtime-fetch-core.js/)
  } finally { await rm(root, { recursive: true, force: true }) }
})
