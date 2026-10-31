import { createHash } from "node:crypto"
import { lstat, readFile, realpath } from "node:fs/promises"
import path from "node:path"
import { buildAdmittedBundles } from "./bundle-core.mjs"
import { ClientBundleError } from "./bundle-errors.mjs"
import { verifyPinnedSource } from "./bundle-pinned.mjs"

const fixedNode = "9d050fd455b56426e25d4d603c7c501cbb2630348e836cf221dcce748e90588a"
const fixedCompiler = "e2dc9a52440a2a34f09434a2f4843cb1e30f84e40dcf238976ec61ef8cd7f36a"
const zodPackage = "c630bd10b52dcf71c112a2bf78dbf2734b9db58d62de663b8d86c2ec2c8cda2e"
const esbuildPackage = "d55d1d19fcc5b6079e4a71dd4111340c79c682bc36835ac7058a0c364c7db58a"
const digest = (bytes) => createHash("sha256").update(bytes).digest("hex")
const controller = "examples/software-factory/controller/src/lib/"

async function checkPackage(file, name, version, expected) {
  try {
    if (await realpath(file) !== file || !(await lstat(file)).isFile()) throw new Error()
    const bytes = await readFile(file)
    const actual = JSON.parse(bytes)
    if (actual.name !== name || actual.version !== version || digest(bytes) !== expected) throw new Error()
  } catch { throw new ClientBundleError("client_bundle_package_invalid") }
}

// The public interface has no source admission, compiler option or synthetic
// override. Its manifest describes only this build, never historical artifacts.
export async function buildClientBundles(options) {
  const names = ["sourceRoot", "nodeExecutable", "compilerExecutable", "outputDir"]
  if (!options || Object.keys(options).length !== names.length || names.some((name) => typeof options[name] !== "string" || options[name].includes("\0") || !path.isAbsolute(options[name]) || path.normalize(options[name]) !== options[name])) throw new ClientBundleError("client_bundle_options_invalid")
  const source = await verifyPinnedSource(options.sourceRoot)
  try {
    if (process.platform !== "darwin" || process.arch !== "arm64" || process.version !== "v24.20.0" || await realpath(options.nodeExecutable) !== process.execPath || digest(await readFile(options.nodeExecutable)) !== fixedNode || digest(await readFile(options.compilerExecutable)) !== fixedCompiler) throw new Error()
  } catch { throw new ClientBundleError("client_bundle_tool_invalid") }
  const zodRoot = path.join(options.sourceRoot, "node_modules/.pnpm/zod@4.4.3/node_modules/zod")
  const compilerPackage = path.join(options.sourceRoot, "node_modules/.pnpm/esbuild@0.28.1/node_modules/esbuild/package.json")
  await checkPackage(path.join(zodRoot, "package.json"), "zod", "4.4.3", zodPackage)
  await checkPackage(compilerPackage, "esbuild", "0.28.1", esbuildPackage)
  return buildAdmittedBundles({
    sourceRoot: options.sourceRoot, compilerExecutable: options.compilerExecutable, outputDir: options.outputDir, zodRoot,
    provenance: "original_pinned_client", source,
    tools: { node: { version: "v24.20.0", sha256: fixedNode }, esbuild: { version: "0.28.1", sha256: fixedCompiler } },
    packages: { zod: { version: "4.4.3", packageJsonSha256: zodPackage }, esbuild: { version: "0.28.1", packageJsonSha256: esbuildPackage } },
    packageFiles: [{ path: compilerPackage, sha256: esbuildPackage }],
    tsconfig: "examples/software-factory/controller/tsconfig.json",
    expectedRuntimeInputs: ["client.ts", "worker/client.ts", "worker/wire.ts", "worker/sse.ts"].map((name) => controller + name),
    entries: [{ name: "controller", path: controller + "client.ts", file: "controller-client.mjs" }, { name: "worker", path: controller + "worker/client.ts", file: "worker-client.mjs" }],
  })
}
