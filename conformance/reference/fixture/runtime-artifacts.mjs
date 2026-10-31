import assert from "node:assert/strict"
import { readFile, readdir, writeFile } from "node:fs/promises"
import { dirname, join, relative, resolve } from "node:path"
import { fileURLToPath } from "node:url"
import { sha256 } from "./harness.mjs"

const scriptPath = fileURLToPath(import.meta.url)
const repositoryRoot = resolve(dirname(scriptPath), "../../..")
const wrapperPath = "packages/cli/dist/lib/dev/runtime-fetch-handler.js"
const corePath = "packages/cli/dist/lib/dev/runtime-fetch-core.js"

async function visitDist(root, prefix) {
  const files = []
  let entries
  try { entries = await readdir(join(root, prefix), { withFileTypes: true }) }
  catch (error) { if (error.code === "ENOENT") return files; throw error }
  for (const entry of entries) {
    const path = `${prefix}/${entry.name}`
    if (entry.isDirectory()) files.push(...await visitDist(root, path))
    else if (entry.isFile() && /\.(?:js|mjs|cjs|json)$/.test(entry.name)) {
      const bytes = await readFile(join(root, path))
      files.push({ path, sha256: sha256(bytes), bytes: bytes.length })
    }
  }
  return files
}

export async function collectRuntimeArtifacts(referenceRoot) {
  const files = []
  for (const entry of await readdir(join(referenceRoot, "packages"), { withFileTypes: true }))
    if (entry.isDirectory()) files.push(...await visitDist(referenceRoot, `packages/${entry.name}/dist`))
  files.sort((a, b) => a.path < b.path ? -1 : a.path > b.path ? 1 : 0)
  const runtimeWrapper = files.find(file => file.path === wrapperPath)
  const runtimeCore = files.find(file => file.path === corePath)
  if (!runtimeWrapper) throw new Error(`Required runtime artifact missing: ${wrapperPath}`)
  if (!runtimeCore) throw new Error(`Required runtime artifact missing: ${corePath}`)
  return {
    coverage_scope: "All regular packages/*/dist .js/.mjs/.cjs/.json files; provenance superset, not a runtime import-closure claim",
    inventory_formula: "SHA256(UTF8(JSON.stringify(files))) where files are sorted by ASCII path and each entry has ordered keys path, sha256, bytes",
    files,
    inventory_sha256: sha256(JSON.stringify(files)),
    runtime_wrapper: runtimeWrapper,
    runtime_core: runtimeCore,
  }
}

export async function createRuntimeArtifactReceipt({ referenceRoot, recordingPaths = [], observationTiming = "post_capture_inventory" }) {
  const { verifyReferenceRoot } = await import("./capture.mjs")
  const reference = await verifyReferenceRoot(referenceRoot)
  const inventory = await collectRuntimeArtifacts(referenceRoot)
  const links = []
  for (const path of recordingPaths) {
    const bytes = await readFile(path)
    const recording = JSON.parse(bytes.toString("utf8"))
    assert.deepEqual(recording.reference, reference, "Recording reference identity differs from current pinned checkout")
    assert.equal(recording.runtime_entry.path, inventory.runtime_wrapper.path, "Recording runtime wrapper path differs")
    assert.equal(recording.runtime_entry.sha256, inventory.runtime_wrapper.sha256, "Recording runtime wrapper hash differs from current built artifact")
    links.push({ path: relative(repositoryRoot, resolve(path)), sha256: sha256(bytes), runtime_wrapper_sha256: recording.runtime_entry.sha256 })
  }
  return {
    format_version: 1,
    evidence_classification: "built_runtime_artifact_inventory",
    reference,
    observation_timing: observationTiming,
    assurance: observationTiming === "post_capture_inventory"
      ? "Inventory observed after existing captures. Their recorded wrapper hashes match. Legacy recordings did not contemporaneously hash transitive artifacts; this sidecar does not retroactively assert that stronger evidence."
      : "Inventory collected before import and checked again after capture; reference identity and inventory must match across capture.",
    ...inventory,
    recording_links: links,
    inventory_script: { path: relative(repositoryRoot, scriptPath), sha256: sha256(await readFile(scriptPath)) },
    inventory_dependencies: await Promise.all(["harness.mjs", "capture.mjs"].map(async path => ({ path: `conformance/reference/fixture/${path}`, sha256: sha256(await readFile(join(dirname(scriptPath), path))) }))),
  }
}

async function main(args) {
  const options = { recordingPaths: [] }
  let output
  for (let index = 0; index < args.length; index += 2) {
    const value = args[index + 1]
    if (!value) throw new Error("Usage: runtime-artifacts.mjs --reference-root PATH --recording PATH [--recording PATH] --output PATH")
    if (args[index] === "--reference-root") options.referenceRoot = resolve(value)
    else if (args[index] === "--recording") options.recordingPaths.push(resolve(value))
    else if (args[index] === "--output") output = resolve(value)
    else throw new Error(`Unknown argument: ${args[index]}`)
  }
  if (!options.referenceRoot || !output) throw new Error("--reference-root and --output are required")
  if (options.recordingPaths.includes(output)) throw new Error("Artifact inventory cannot overwrite an input recording")
  const receipt = await createRuntimeArtifactReceipt(options)
  await writeFile(output, `${JSON.stringify(receipt, null, 2)}\n`)
  process.stdout.write(`${JSON.stringify({ files: receipt.files.length, inventory_sha256: receipt.inventory_sha256, runtime_wrapper: receipt.runtime_wrapper, runtime_core: receipt.runtime_core, recording_links: receipt.recording_links }, null, 2)}\n`)
}

if (process.argv[1] && resolve(process.argv[1]) === scriptPath)
  main(process.argv.slice(2)).catch(error => { process.stderr.write(`${error.stack ?? error}\n`); process.exitCode = 1 })
