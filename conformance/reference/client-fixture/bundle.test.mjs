import assert from "node:assert/strict"
import { execFile } from "node:child_process"
import { createHash } from "node:crypto"
import { mkdtemp, mkdir, readFile, realpath, rm, stat, symlink, writeFile } from "node:fs/promises"
import { tmpdir } from "node:os"
import path from "node:path"
import { promisify } from "node:util"
import { test } from "node:test"
import { buildAdmittedBundles } from "./bundle-core.mjs"
import { buildClientBundles } from "./bundle.mjs"
import { validatePinnedObservation, verifyPinnedSource } from "./bundle-pinned.mjs"

const execute = promisify(execFile)
const compilerExecutable = "/private/tmp/b4run-grant-reference-8acac534/node_modules/.pnpm/@esbuild+darwin-arm64@0.28.1/node_modules/@esbuild/darwin-arm64/bin/esbuild"
const zodRoot = "/private/tmp/b4run-grant-reference-8acac534/node_modules/.pnpm/zod@4.4.3/node_modules/zod"
const digest = (bytes) => createHash("sha256").update(bytes).digest("hex")

// Authored helper sources only. They are not the original AP clients or an
// actual native corpus. esbuild compiles them; tests do not import the bundles.
async function syntheticGraph(t) {
  const root = await realpath(await mkdtemp(path.join(tmpdir(), "b4-client-bundle-unit-")))
  t.after(() => rm(root, { recursive: true, force: true }))
  const sourceRoot = path.join(root, "source")
  const files = {
    "controller.ts": 'import { randomUUID } from "node:crypto"; export const createControllerClient = () => ({ synthetic: randomUUID }); export class ControllerHttpError extends Error {}\n',
    "worker.ts": 'import { setTimeout } from "node:timers/promises"; import { schema } from "./wire.js"; import { parseSse } from "./sse.js"; export const createHttpWorkerClient = () => ({ synthetic: schema, parseSse, setTimeout }); export class WorkerHttpError extends Error {}\n',
    "wire.ts": 'import { z } from "zod"; export const schema = z.object({ synthetic: z.literal(true) });\n',
    "sse.ts": 'export async function* parseSse() { yield { synthetic: true }; }\n',
  }
  await mkdir(path.join(sourceRoot, "node_modules"), { recursive: true, mode: 0o700 })
  await symlink(zodRoot, path.join(sourceRoot, "node_modules", "zod"))
  for (const [name, text] of Object.entries(files)) {
    await writeFile(path.join(sourceRoot, name), text, { flag: "wx", mode: 0o600 })
  }
  const { stdout, stderr } = await execute(compilerExecutable, ["controller.ts", "--bundle", "--platform=node", "--format=esm", "--log-level=error"], { cwd: sourceRoot, env: {}, encoding: "buffer" })
  assert.equal(stderr.length, 0)
  assert.ok(stdout.includes(Buffer.from("createControllerClient")), "synthetic source and installed compiler must work before the builder assertion")
  return {
    sourceRoot, compilerExecutable, zodRoot,
    outputDir: path.join(root, "bundles"),
    provenance: "synthetic_unit",
    source: { commit: null, files: Object.entries(files).map(([name, text]) => ({ path: name, sha256: digest(text), bytes: Buffer.byteLength(text) })) },
    tools: { node: { version: process.version, sha256: digest(await readFile(process.execPath)) }, esbuild: { version: "0.28.1", sha256: digest(await readFile(compilerExecutable)) } },
    packages: { zod: { version: "4.4.3", packageJsonSha256: digest(await readFile(path.join(zodRoot, "package.json"))) } },
    entries: [{ name: "controller", path: "controller.ts", file: "controller-client.mjs" }, { name: "worker", path: "worker.ts", file: "worker-client.mjs" }],
  }
}

test("synthetic admitted sources produce both complete ESM bundle provenance records", async (t) => {
  const options = await syntheticGraph(t)
  const result = await buildAdmittedBundles(options)
  assert.equal(result?.version, 1)
  assert.equal(result.provenance, "synthetic_unit")
  assert.equal(result.outputs.length, 2)
  assert.deepEqual(result.outputs.map((o) => o.file), ["controller-client.mjs", "worker-client.mjs"])
  for (const output of result.outputs) {
    const bytes = await readFile(path.join(options.outputDir, output.file))
    assert.equal(output.bytes, bytes.length)
    assert.equal(output.sha256, digest(bytes))
  }
  assert.ok(result.inputs.some((input) => input.kind === "zod"))
  assert.ok(result.inputs.some((input) => input.kind === "source" && input.path === "wire.ts"))
  assert.deepEqual(JSON.parse(await readFile(path.join(options.outputDir, "bundle-manifest.json"))), result)
  assert.equal((await stat(options.outputDir)).mode & 0o777, 0o700)
  for (const file of ["controller-client.mjs", "worker-client.mjs", "bundle-manifest.json"]) {
    assert.equal((await stat(path.join(options.outputDir, file))).mode & 0o777, 0o600)
  }
})

const refuses = async (options, code) => assert.rejects(buildAdmittedBundles(options), (error) => {
  assert.equal(error.name, "ClientBundleError")
  assert.equal(error.message, code)
  assert.equal(error.cause, undefined)
  return true
})

test("a reused output directory is refused without replacing its existing artifact", async (t) => {
  const options = await syntheticGraph(t)
  await mkdir(options.outputDir)
  const file = path.join(options.outputDir, "controller-client.mjs")
  await writeFile(file, "retained sentinel")
  await refuses(options, "client_bundle_output_invalid")
  assert.equal(await readFile(file, "utf8"), "retained sentinel")
})

test("an output symlink is refused without following it", async (t) => {
  const options = await syntheticGraph(t)
  const target = path.join(path.dirname(options.outputDir), "foreign")
  await mkdir(target)
  await symlink(target, options.outputDir)
  await refuses(options, "client_bundle_output_invalid")
})

test("an output parent symlink is refused before creation", async (t) => {
  const options = await syntheticGraph(t)
  const alias = path.join(path.dirname(options.outputDir), "alias")
  await symlink(path.dirname(options.outputDir), alias)
  options.outputDir = path.join(alias, "bundles")
  await refuses(options, "client_bundle_output_invalid")
})

test("changed admitted source bytes refuse before compiling a bundle", async (t) => {
  const options = await syntheticGraph(t)
  await writeFile(path.join(options.sourceRoot, "wire.ts"), "export const schema = null;")
  await refuses(options, "client_bundle_source_invalid")
})

test("changed compiler bytes cannot be admitted by an old digest", async (t) => {
  const options = await syntheticGraph(t)
  options.tools.esbuild.sha256 = "0".repeat(64)
  await refuses(options, "client_bundle_tool_invalid")
})

test("wrong declared installed compiler version is refused", async (t) => {
  const options = await syntheticGraph(t)
  options.tools.esbuild.version = "0.0.0"
  await refuses(options, "client_bundle_tool_invalid")
})

test("wrong installed Zod version or package bytes are refused", async (t) => {
  const options = await syntheticGraph(t)
  options.packages.zod.version = "0.0.0"
  await refuses(options, "client_bundle_package_invalid")
  options.packages.zod.version = "4.4.3"
  options.packages.zod.packageJsonSha256 = "0".repeat(64)
  await refuses(options, "client_bundle_package_invalid")
})

test("an unexpected transitive source input is refused instead of omitted from provenance", async (t) => {
  const options = await syntheticGraph(t)
  const extra = path.join(options.sourceRoot, "extra.ts")
  await writeFile(extra, "export const extra = true;\n")
  const text = options.source.files.find((f) => f.path === "controller.ts")
  const bytes = Buffer.from('export { extra } from "./extra.js";\n')
  await writeFile(path.join(options.sourceRoot, "controller.ts"), bytes)
  text.sha256 = digest(bytes); text.bytes = bytes.length
  await refuses(options, "client_bundle_input_invalid")
})

test("an unselected external Node builtin is refused", async (t) => {
  const options = await syntheticGraph(t)
  const bytes = Buffer.from('export { readFile } from "node:fs/promises";\n')
  await writeFile(path.join(options.sourceRoot, "controller.ts"), bytes)
  Object.assign(options.source.files.find((f) => f.path === "controller.ts"), { sha256: digest(bytes), bytes: bytes.length })
  await refuses(options, "client_bundle_input_invalid")
})

test("compiler diagnostics become a fixed refusal without source text", async (t) => {
  const options = await syntheticGraph(t)
  const bytes = Buffer.from("private_synthetic_syntax_marker ! @ @\n")
  await writeFile(path.join(options.sourceRoot, "controller.ts"), bytes)
  Object.assign(options.source.files.find((f) => f.path === "controller.ts"), { sha256: digest(bytes), bytes: bytes.length })
  await refuses(options, "client_bundle_compile_failed")
})

// This observation is a synthetic value, not a Git checkout or original build.
const pinnedObservation = () => ({
  commit: "8acac534ba6eb960b34de217b0da45b8405023af",
  files: [
    ["examples/software-factory/controller/src/lib/client.ts", "4dc62f501187af8156212c57730f15e528a61046", "b78f89ec1e5cb29b0a8d0ed833868805001355cada42ed7597f91d1af7a01407", 4880],
    ["examples/software-factory/controller/src/lib/worker/client.ts", "98ee0e811e91f13b6e7f6d7659db5311379bb5a4", "195282a5a01e3c7dde235711f9710745195912572037a88638261621a705f374", 10775],
    ["examples/software-factory/controller/src/lib/worker/wire.ts", "0c7696b4c0bbb0705aecbb272af40d3c3a1c8af6", "2fd6ff07389bc72477552f5cedbc4189f314a1da2d0de9d23f6ed05c52643574", 1970],
    ["examples/software-factory/controller/src/lib/worker/sse.ts", "471e351e293cd428f89d1a2e317112b94c897335", "f7bbcb5c6eec64a9fccc98f8242e53d901e4c48b5739ee15dc5b359013ad19fc", 2491],
    ["examples/software-factory/controller/package.json", "d2836575175fc979ad72a881291fdca104931304", "f2f00f3395ec0389425465d67454032e3bcdbd1ffa21093aecdfc95a8bde2a52", 1179],
    ["examples/software-factory/controller/tsconfig.json", "2b485dd21e8ab15249da95e41bfb2f158969e768", "9b8f7479c2ecb20e03288d400fb3fbd1eef37014a1b2895bcc4131812212f37a", 583],
    ["package.json", "75d1e52cf30aea7a0c41b55e9e083b421a575b39", "9f965f5b995cd43350a5793948d74a61cf1acc4454f3ba41b557d0a286a11e45", 5714],
    ["pnpm-lock.yaml", "b3f361cca058abd4029dd0e69c1e719f401963e1", "46be0b081db024914c6da5d861c9416392abeea3bd44cbc10b6fba0840494ca0", 625496],
    ["tsconfig.json", "4bb0f9ed220436e6205104021a82e24f6c0bf485", "71a38113db8909197e9b8c0f58502050055feab24b595285546b3e6a5449fa81", 300],
  ].map(([path, gitBlob, sha256, bytes]) => ({ path, gitBlob, sha256, bytes })),
})

test("the complete fixed pin and original blob observation is admitted", () => {
  assert.equal(validatePinnedObservation(pinnedObservation()), true)
})

test("changed Git pin or blob is a fixed refusal", () => {
  for (const change of [(o) => { o.commit = "0".repeat(40) }, (o) => { o.files[0].gitBlob = "0".repeat(40) }, (o) => { o.files[0].sha256 = "0".repeat(64) }]) {
    const observation = pinnedObservation(); change(observation)
    assert.throws(() => validatePinnedObservation(observation), { name: "ClientBundleError", message: "client_bundle_pin_invalid" })
  }
})

test("an incomplete or duplicate pinned source observation is refused", () => {
  for (const change of [(o) => o.files.pop(), (o) => o.files.push(o.files[0])]) {
    const observation = pinnedObservation(); change(observation)
    assert.throws(() => validatePinnedObservation(observation), { name: "ClientBundleError", message: "client_bundle_pin_invalid" })
  }
})

test("public builder refuses synthetic roots before any original-client compilation", async (t) => {
  const options = await syntheticGraph(t)
  await assert.rejects(buildClientBundles({ sourceRoot: options.sourceRoot, compilerExecutable, nodeExecutable: process.execPath, outputDir: options.outputDir }), { name: "ClientBundleError", message: "client_bundle_pin_invalid" })
  await assert.rejects(stat(options.outputDir), { code: "ENOENT" })
})

// A synthetic executable delegates to the actual installed compiler, then
// changes only its private unit metafile. This is not an original build tool.
async function compilerFixture(options, action) {
  const file = path.join(path.dirname(options.outputDir), "synthetic-compiler.mjs")
  const script = `#!${process.execPath}\nimport { execFileSync } from "node:child_process"; import { readFileSync, writeFileSync } from "node:fs";
const args = process.argv.slice(2); if (args[0] === "--version") { process.stdout.write("0.28.1\\n"); } else {
execFileSync(${JSON.stringify(compilerExecutable)}, args, { env: {}, stdio: "pipe" });
const meta = args.find((arg) => arg.startsWith("--metafile=")).slice(11); const document = JSON.parse(readFileSync(meta));
${action}
writeFileSync(meta, JSON.stringify(document)); }
`
  await writeFile(file, script, { flag: "wx", mode: 0o700 })
  options.compilerExecutable = file
  options.tools.esbuild.sha256 = digest(script)
}

test("an incomplete actual compiler output input map cannot produce a success manifest", async (t) => {
  const options = await syntheticGraph(t)
  await compilerFixture(options, 'for (const output of Object.values(document.outputs)) delete output.inputs[args[0]];')
  await refuses(options, "client_bundle_input_invalid")
  await assert.rejects(stat(options.outputDir), { code: "ENOENT" })
})

test("a malformed actual compiler metafile is refused", async (t) => {
  const options = await syntheticGraph(t)
  await compilerFixture(options, 'delete document.outputs;')
  await refuses(options, "client_bundle_input_invalid")
})

test("actual source mutation during the compiler process refuses final provenance", async (t) => {
  const options = await syntheticGraph(t)
  await compilerFixture(options, 'if (args[0] === "worker.ts") writeFileSync("controller.ts", "export const changed = true;\\n");')
  await refuses(options, "client_bundle_source_invalid")
  await assert.rejects(stat(options.outputDir), { code: "ENOENT" })
})

test("wrong Node admission and invalid public options are fixed refusals", async (t) => {
  const options = await syntheticGraph(t)
  options.tools.node.sha256 = "0".repeat(64)
  await refuses(options, "client_bundle_tool_invalid")
  await assert.rejects(buildClientBundles({ sourceRoot: ".", nodeExecutable: process.execPath, compilerExecutable, outputDir: options.outputDir }), { name: "ClientBundleError", message: "client_bundle_options_invalid" })
})

test("read-only original source verification admits the actual fixed Git tree without building or importing clients", async () => {
  const observation = await verifyPinnedSource("/private/tmp/b4run-grant-reference-8acac534")
  assert.deepEqual(observation, pinnedObservation())
})
