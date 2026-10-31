import { execFile } from "node:child_process"
import { createHash } from "node:crypto"
import { lstat, mkdir, mkdtemp, readFile, readdir, realpath, rm, writeFile } from "node:fs/promises"
import path from "node:path"
import { promisify } from "node:util"
import { verifyPinnedSource } from "./bundle-pinned.mjs"
import { ClientBundleError } from "./bundle-errors.mjs"

const execute = promisify(execFile)
const allowedExternals = new Set(["node:crypto", "node:timers/promises"])
const digest = (bytes) => createHash("sha256").update(bytes).digest("hex")
const refuse = (code) => { throw new ClientBundleError(code) }
const sorted = (values) => [...values].sort()
const relativeFile = (value) => typeof value === "string" && value.length > 0 && !value.includes("\\") && !path.isAbsolute(value) && !value.split("/").some((part) => part === "" || part === "." || part === "..")
const absolutePath = (value) => typeof value === "string" && !value.includes("\0") && path.isAbsolute(value) && path.normalize(value) === value
const sameRecord = (a, b) => a?.bytes === b?.bytes && a?.sha256 === b?.sha256 && a !== undefined && b !== undefined

async function fixedRead(file, code) {
  try {
    if (!(await lstat(file)).isFile()) refuse(code)
    const bytes = await readFile(file)
    return { bytes: bytes.length, sha256: digest(bytes) }
  } catch { refuse(code) }
}

async function canonicalDirectory(directory, code) {
  try {
    if (!absolutePath(directory) || (await realpath(directory)) !== directory || !(await lstat(directory)).isDirectory()) refuse(code)
  } catch { refuse(code) }
}

async function fileTree(root, code) {
  const records = new Map()
  async function visit(directory) {
    for (const entry of await readdir(directory, { withFileTypes: true })) {
      const file = path.join(directory, entry.name)
      if (entry.isDirectory()) await visit(file)
      else if (entry.isFile()) records.set(file, await fixedRead(file, code))
      else refuse(code)
    }
  }
  try { await visit(root) } catch { refuse(code) }
  return records
}

function checkOptions(options) {
  if (!options || !absolutePath(options.sourceRoot) || !absolutePath(options.compilerExecutable) || !absolutePath(options.zodRoot) || !absolutePath(options.outputDir)) refuse("client_bundle_options_invalid")
  if (!["synthetic_unit", "original_pinned_client"].includes(options.provenance) || !Array.isArray(options.source?.files) || !options.source.files.length || !options.tools?.node || !options.tools.esbuild || !options.packages?.zod) refuse("client_bundle_options_invalid")
  if (!Array.isArray(options.entries) || options.entries.length !== 2 || options.entries[0].name !== "controller" || options.entries[0].file !== "controller-client.mjs" || options.entries[1].name !== "worker" || options.entries[1].file !== "worker-client.mjs") refuse("client_bundle_options_invalid")
  const paths = new Set()
  for (const record of options.source.files) {
    if (!relativeFile(record.path) || paths.has(record.path) || !/^[a-f0-9]{64}$/.test(record.sha256) || !Number.isSafeInteger(record.bytes) || record.bytes < 0) refuse("client_bundle_options_invalid")
    paths.add(record.path)
  }
  if (options.entries.some((entry) => !relativeFile(entry.path) || !paths.has(entry.path))) refuse("client_bundle_options_invalid")
}

async function checkTools(options) {
  const expected = options.tools
  if (expected.node.version !== "v24.20.0" || process.version !== expected.node.version || expected.esbuild.version !== "0.28.1") refuse("client_bundle_tool_invalid")
  const node = await fixedRead(process.execPath, "client_bundle_tool_invalid")
  const compiler = await fixedRead(options.compilerExecutable, "client_bundle_tool_invalid")
  if (node.sha256 !== expected.node.sha256 || compiler.sha256 !== expected.esbuild.sha256) refuse("client_bundle_tool_invalid")
  try {
    const result = await execute(options.compilerExecutable, ["--version"], { env: {}, encoding: "utf8", timeout: 10000, maxBuffer: 1024 })
    if (result.stdout !== "0.28.1\n" || result.stderr !== "") refuse("client_bundle_tool_invalid")
  } catch { refuse("client_bundle_tool_invalid") }
  return new Map([[process.execPath, node], [options.compilerExecutable, compiler]])
}

async function admittedInputs(options) {
  await canonicalDirectory(options.sourceRoot, "client_bundle_source_invalid")
  await canonicalDirectory(options.zodRoot, "client_bundle_package_invalid")
  const source = new Map()
  for (const expected of options.source.files) {
    const file = path.join(options.sourceRoot, expected.path)
    try { if ((await realpath(file)) !== file) refuse("client_bundle_source_invalid") } catch { refuse("client_bundle_source_invalid") }
    const record = await fixedRead(file, "client_bundle_source_invalid")
    if (!sameRecord(record, expected)) refuse("client_bundle_source_invalid")
    source.set(file, record)
  }
  const packageFile = path.join(options.zodRoot, "package.json")
  const packageRecord = await fixedRead(packageFile, "client_bundle_package_invalid")
  try {
    const packageJson = JSON.parse(await readFile(packageFile, "utf8"))
    if (options.packages.zod.version !== "4.4.3" || packageJson.version !== "4.4.3" || packageJson.name !== "zod" || packageRecord.sha256 !== options.packages.zod.packageJsonSha256) refuse("client_bundle_package_invalid")
  } catch { refuse("client_bundle_package_invalid") }
  const zod = await fileTree(options.zodRoot, "client_bundle_package_invalid")
  if (!sameRecord(zod.get(packageFile), packageRecord)) refuse("client_bundle_package_invalid")
  return { source, zod }
}

async function assertUnchanged(records, code) {
  for (const [file, before] of records) if (!sameRecord(await fixedRead(file, code), before)) refuse(code)
}

// Each compiler input/import and its sole staged output must be accounted for.
// This internal helper admits synthetic graphs only in unit tests. The public
// builder constructs original-only admissions after checking the fixed Git pin.
async function accountMetafile(metadata, bytes, entry, options, admitted, compiledFile) {
  if (!metadata || typeof metadata.inputs !== "object" || metadata.inputs === null || typeof metadata.outputs !== "object" || metadata.outputs === null) refuse("client_bundle_input_invalid")
  const names = Object.keys(metadata.outputs)
  if (names.length !== 1 || path.resolve(options.sourceRoot, names[0]) !== compiledFile) refuse("client_bundle_input_invalid")
  const output = metadata.outputs[names[0]]
  if (output.bytes !== bytes.length || output.entryPoint !== entry.path || !output.inputs || !Array.isArray(output.imports)) refuse("client_bundle_input_invalid")
  const inputs = new Map()
  for (const [name, details] of Object.entries(metadata.inputs)) {
    if (!details || !Array.isArray(details.imports)) refuse("client_bundle_input_invalid")
    let file
    try { file = await realpath(path.resolve(options.sourceRoot, name)) } catch { refuse("client_bundle_input_invalid") }
    const kind = admitted.source.has(file) ? "source" : admitted.zod.has(file) ? "zod" : null
    if (!kind || inputs.has(file)) refuse("client_bundle_input_invalid")
    const before = admitted[kind === "source" ? "source" : "zod"].get(file)
    if (details.bytes !== before.bytes) refuse("client_bundle_input_invalid")
    inputs.set(file, { kind, path: path.relative(kind === "source" ? options.sourceRoot : options.zodRoot, file).split(path.sep).join("/"), ...before })
    for (const item of details.imports) {
      if (!item || typeof item.path !== "string") refuse("client_bundle_input_invalid")
      if (item.external && !allowedExternals.has(item.path)) refuse("client_bundle_input_invalid")
      if (!item.external && !Object.hasOwn(metadata.inputs, item.path)) refuse("client_bundle_input_invalid")
    }
  }
  if (!Object.hasOwn(metadata.inputs, entry.path)) refuse("client_bundle_input_invalid")
  for (const [name, details] of Object.entries(output.inputs)) {
    if (!Object.hasOwn(metadata.inputs, name) || !Number.isSafeInteger(details?.bytesInOutput) || details.bytesInOutput < 0) refuse("client_bundle_input_invalid")
  }
  if (Object.keys(output.inputs).length !== Object.keys(metadata.inputs).length) refuse("client_bundle_input_invalid")
  const externalImports = []
  for (const item of output.imports) {
    if (!item?.external || !allowedExternals.has(item.path)) refuse("client_bundle_input_invalid")
    externalImports.push(item.path)
  }
  return { inputs, externalImports: sorted(new Set(externalImports)) }
}

export async function buildAdmittedBundles(options) {
  checkOptions(options)
  let stage
  let code = "client_bundle_output_invalid"
  try {
    await canonicalDirectory(path.dirname(options.outputDir), code)
    try { await lstat(options.outputDir); refuse(code) } catch (error) { if (error.code !== "ENOENT") refuse(code) }
    const tools = await checkTools(options)
    const admitted = await admittedInputs(options)
    const packageFiles = new Map()
    for (const expected of options.packageFiles ?? []) {
      const record = await fixedRead(expected.path, "client_bundle_package_invalid")
      if (record.sha256 !== expected.sha256) refuse("client_bundle_package_invalid")
      packageFiles.set(expected.path, record)
    }
    stage = await mkdtemp(path.join(path.dirname(options.outputDir), ".client-bundle-stage-"))
    const allInputs = new Map()
    const bundles = []
    for (const entry of options.entries) {
      const metadataFile = path.join(stage, `${entry.name}.json`)
      const compiledFile = path.join(stage, entry.file)
      // Reserve our staging artifact privately before the compiler writes it.
      await writeFile(metadataFile, "", { flag: "wx", mode: 0o600 })
      await writeFile(compiledFile, "", { flag: "wx", mode: 0o600 })
      let bytes
      code = "client_bundle_compile_failed"
      try {
        const args = [entry.path, "--bundle", "--platform=node", "--format=esm", "--charset=utf8", "--log-level=error", `--metafile=${metadataFile}`, `--outfile=${compiledFile}`]
        if (options.tsconfig) args.push(`--tsconfig=${path.join(options.sourceRoot, options.tsconfig)}`)
        const result = await execute(options.compilerExecutable, args, { cwd: options.sourceRoot, env: {}, encoding: "buffer", timeout: 30000, maxBuffer: 8 * 1024 * 1024 })
        if (result.stderr.length || result.stdout.length) refuse(code)
        bytes = await readFile(compiledFile)
        if (!bytes.length) refuse(code)
      } catch { refuse(code) }
      code = "client_bundle_input_invalid"
      let metadata
      try { metadata = JSON.parse(await readFile(metadataFile, "utf8")) } catch { refuse(code) }
      const accounted = await accountMetafile(metadata, bytes, entry, options, admitted, compiledFile)
      for (const [file, record] of accounted.inputs) allInputs.set(file, record)
      bundles.push({ bytes, output: { file: entry.file, entry: entry.name, source: entry.path, bytes: bytes.length, sha256: digest(bytes), inputs: sorted([...accounted.inputs.values()].map((input) => `${input.kind}:${input.path}`)), externalImports: accounted.externalImports } })
    }
    await assertUnchanged(admitted.source, "client_bundle_source_invalid")
    await assertUnchanged(admitted.zod, "client_bundle_package_invalid")
    await assertUnchanged(tools, "client_bundle_tool_invalid")
    await assertUnchanged(packageFiles, "client_bundle_package_invalid")
    for (const name of options.expectedRuntimeInputs ?? []) if (![...allInputs.values()].some((input) => input.kind === "source" && input.path === name)) refuse("client_bundle_input_invalid")
    if (options.provenance === "original_pinned_client") await verifyPinnedSource(options.sourceRoot)
    const manifest = { version: 1, provenance: options.provenance, source: structuredClone(options.source), tools: structuredClone(options.tools), packages: structuredClone(options.packages), build: { platform: "node", format: "esm", bundle: true, charset: "utf8", minify: false, sourcemap: false, tsconfig: options.tsconfig ?? null, externalImports: sorted(allowedExternals) }, inputs: [...allInputs.values()].sort((a, b) => `${a.kind}:${a.path}`.localeCompare(`${b.kind}:${b.path}`, "en")), outputs: bundles.map((bundle) => bundle.output) }
    code = "client_bundle_output_invalid"
    await canonicalDirectory(path.dirname(options.outputDir), code)
    await mkdir(options.outputDir, { mode: 0o700 })
    for (const bundle of bundles) await writeFile(path.join(options.outputDir, bundle.output.file), bundle.bytes, { flag: "wx", mode: 0o600 })
    await writeFile(path.join(options.outputDir, "bundle-manifest.json"), JSON.stringify(manifest, null, 2) + "\n", { flag: "wx", mode: 0o600 })
    return manifest
  } catch (error) {
    if (error instanceof ClientBundleError) throw error
    refuse(code)
  } finally {
    if (stage) {
      try { await rm(stage, { recursive: true, force: true }) } catch { refuse("client_bundle_output_invalid") }
    }
  }
}
