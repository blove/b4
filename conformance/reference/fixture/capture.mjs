import { execFile } from "node:child_process"
import assert from "node:assert/strict"
import { cp, mkdir, mkdtemp, readFile, readdir, rm, writeFile } from "node:fs/promises"
import { tmpdir } from "node:os"
import { join, resolve } from "node:path"
import { fileURLToPath, pathToFileURL } from "node:url"
import { promisify } from "node:util"
import { recordResponse, sha256 } from "./harness.mjs"
import { collectRuntimeArtifacts, createRuntimeArtifactReceipt } from "./runtime-artifacts.mjs"

const exec = promisify(execFile)
export const PINNED_COMMIT = "8acac534ba6eb960b34de217b0da45b8405023af"
const PINNED_LOCKFILE_SHA256 = "46be0b081db024914c6da5d861c9416392abeea3bd44cbc10b6fba0840494ca0"
const fixtureRoot = fileURLToPath(new URL(".", import.meta.url))

export function parseArguments(args) {
  const options = {}
  for (let index = 0; index < args.length; index += 2) {
    const flag = args[index]
    const value = args[index + 1]
    if (!value || !["--reference-root", "--output"].includes(flag))
      throw new Error("Usage: capture.mjs --reference-root PATH --output PATH")
    options[flag === "--reference-root" ? "referenceRoot" : "output"] = resolve(value)
  }
  if (!options.referenceRoot) throw new Error("--reference-root is required")
  if (!options.output) throw new Error("--output is required")
  return options
}

export async function verifyReferenceRoot(referenceRoot) {
  let stdout
  try { ({ stdout } = await exec("git", ["-C", referenceRoot, "rev-parse", "HEAD"])) }
  catch (cause) { throw new Error(`Reference checkout must be at ${PINNED_COMMIT}; HEAD could not be read`, { cause }) }
  if (stdout.trim() !== PINNED_COMMIT)
    throw new Error(`Reference checkout must be at ${PINNED_COMMIT}; found ${stdout.trim()}`)
  await exec("git", ["-C", referenceRoot, "diff", "--exit-code", "HEAD", "--"])
  const lockfileSha256 = sha256(await readFile(join(referenceRoot, "pnpm-lock.yaml")))
  if (lockfileSha256 !== PINNED_LOCKFILE_SHA256) throw new Error("Pinned lockfile bytes differ")
  return { commit: stdout.trim(), lockfile_sha256: lockfileSha256 }
}

async function hashFiles(root, prefix = "") {
  const files = []
  for (const entry of (await readdir(join(root, prefix), { withFileTypes: true })).sort((a, b) => a.name.localeCompare(b.name))) {
    const path = prefix ? `${prefix}/${entry.name}` : entry.name
    if (entry.isDirectory()) files.push(...await hashFiles(root, path))
    else if (entry.isFile()) {
      const bytes = await readFile(join(root, path))
      files.push({ path, sha256: sha256(bytes), bytes: bytes.length })
    }
  }
  return files
}

async function dependencyVersions(referenceRoot) {
  const versions = {}
  const packages = [
    ["@b4run/cli", "packages/cli/package.json"],
    ["@b4run/sdk", "packages/cli/node_modules/@b4run/sdk/package.json"],
    ["@b4run/core", "packages/cli/node_modules/@b4run/core/package.json"],
    ["@b4run/langchain", "packages/cli/node_modules/@b4run/langchain/package.json"],
    ["@b4run/sqlite-storage", "packages/cli/node_modules/@b4run/sqlite-storage/package.json"],
    ["@langchain/core", "packages/cli/node_modules/@langchain/core/package.json"],
    ["@langchain/langgraph", "packages/cli/node_modules/@langchain/langgraph/package.json"],
    ["langchain", "packages/cli/node_modules/langchain/package.json"],
    ["typescript", "node_modules/typescript/package.json"],
    ["vitest", "node_modules/vitest/package.json"],
  ]
  for (const [name, path] of packages)
    versions[name] = JSON.parse(await readFile(join(referenceRoot, path), "utf8")).version
  return versions
}

export async function capture({ referenceRoot, output }) {
  const reference = await verifyReferenceRoot(referenceRoot)
  const runtimeArtifacts = await createRuntimeArtifactReceipt({ referenceRoot, observationTiming: "capture_time_inventory" })
  const runtimeEntry = "packages/cli/dist/lib/dev/runtime-fetch-handler.js"
  const runtimeBytes = await readFile(join(referenceRoot, runtimeEntry))
  const { createRuntimeFetchHandler } = await import(pathToFileURL(join(referenceRoot, runtimeEntry)))
  const casesBytes = await readFile(join(fixtureRoot, "cases.json"))
  const cases = JSON.parse(casesBytes.toString("utf8"))
  const files = await hashFiles(join(fixtureRoot, "app"))
  const revision = sha256(JSON.stringify({ app: files, cases_sha256: sha256(casesBytes) }))
  const temporaryRoot = await mkdtemp(join(tmpdir(), "b4-reference-fixture-"))
  const appRoot = join(temporaryRoot, "app")
  await cp(join(fixtureRoot, "app"), appRoot, { recursive: true })
  const originalFetch = globalThis.fetch
  let externalFetchAttempts = 0
  globalThis.fetch = async () => {
    externalFetchAttempts++
    throw new Error("Independent fixture forbids external fetch calls")
  }
  let handler
  const observations = []
  try {
    handler = await createRuntimeFetchHandler({ appRoot, drainDeadlineMs: 250, apSseHeartbeatIntervalMs: 60_000 })
    for (const requestCase of cases) {
      const request = new Request(`http://reference.invalid${requestCase.path}`, {
        method: requestCase.method,
        headers: requestCase.headers,
        body: Buffer.from(requestCase.body, "utf8"),
      })
      const response = await recordResponse(await handler.fetch(request), requestCase.normalization)
      const threadId = /^\/threads\/([^/]+)\/runs\//.exec(requestCase.path)?.[1]
      const settledThread = threadId
        ? await recordResponse(await handler.fetch(new Request(`http://reference.invalid/threads/${threadId}`)), "thread")
        : undefined
      observations.push({
        id: requestCase.id,
        characterization: requestCase.characterization,
        evidence_classification: "runtime_observation",
        request: {
          method: request.method,
          path: requestCase.path,
          headers: [...request.headers.entries()],
          body_utf8: requestCase.body,
          body_base64: Buffer.from(requestCase.body, "utf8").toString("base64"),
          body_sha256: sha256(Buffer.from(requestCase.body, "utf8")),
        },
        response,
        ...(settledThread ? { thread_after: settledThread } : {}),
      })
    }
  } finally {
    try { await handler?.close() } finally {
      globalThis.fetch = originalFetch
      await rm(temporaryRoot, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 })
    }
  }
  if (externalFetchAttempts) throw new Error(`${externalFetchAttempts} prohibited external fetch calls occurred`)
  assert.equal((await collectRuntimeArtifacts(referenceRoot)).inventory_sha256, runtimeArtifacts.inventory_sha256, "Built runtime artifacts changed during capture")
  await verifyReferenceRoot(referenceRoot)
  const recording = {
    format_version: 1,
    evidence_classification: "runtime_observation",
    transport: "in_process_fetch",
    protocol_surface: "Agent Protocol HTTP endpoints; no AG-UI",
    reference,
    runtime_entry: { path: runtimeEntry, sha256: sha256(runtimeBytes) },
    runtime_artifacts: { sidecar_file: "runtime-artifacts.json", inventory_sha256: runtimeArtifacts.inventory_sha256, runtime_core: runtimeArtifacts.runtime_core },
    execution: { node_version: process.version, platform: process.platform, arch: process.arch, external_fetch_attempts: externalFetchAttempts },
    effective_dependency_versions: await dependencyVersions(referenceRoot),
    fixture_application: { revision, files, cases_sha256: sha256(casesBytes), configuration: {}, routes: ["/probe#workflow", "/void#workflow", "/zero#workflow", "/false#workflow", "/empty#workflow", "/fail#workflow", "/validate#workflow"] },
    normalization: { thread: "Only top-level thread_id, created_at and updated_at are replaced; raw bytes are also retained", none: "No normalization" },
    characterization_scope: "Selected CP-001–004 parser/input/workflow terminals. Permission park, cancellation, and TCP transport remain unqualified; CP-001–010 and M0 remain open.",
    observations,
  }
  await mkdir(output, { recursive: true })
  const destination = join(output, "workflow-observations.json")
  const bytes = Buffer.from(`${JSON.stringify(recording, null, 2)}\n`)
  await writeFile(destination, bytes, { flag: "wx" })
  runtimeArtifacts.recording_links = [{ path: "workflow-observations.json", path_base: "sidecar_directory", sha256: sha256(bytes), runtime_wrapper_sha256: recording.runtime_entry.sha256 }]
  await writeFile(join(output, "runtime-artifacts.json"), `${JSON.stringify(runtimeArtifacts, null, 2)}\n`, { flag: "wx" })
  return { path: destination, sha256: sha256(bytes), cases: observations.length, fixture_revision: revision }
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  capture(parseArguments(process.argv.slice(2)))
    .then((receipt) => process.stdout.write(`${JSON.stringify(receipt, null, 2)}\n`))
    .catch((error) => { process.stderr.write(`${error.stack ?? error}\n`); process.exitCode = 1 })
}
