import { execFile } from "node:child_process"
import { createHash } from "node:crypto"
import { lstat, readFile, realpath } from "node:fs/promises"
import path from "node:path"
import { promisify } from "node:util"
import { ClientBundleError } from "./bundle-errors.mjs"

const execute = promisify(execFile)
const digest = (bytes) => createHash("sha256").update(bytes).digest("hex")
export const pinnedCommit = "8acac534ba6eb960b34de217b0da45b8405023af"
const expectedFiles = [
  {
    "path": "examples/software-factory/controller/src/lib/client.ts",
    "gitBlob": "4dc62f501187af8156212c57730f15e528a61046",
    "sha256": "b78f89ec1e5cb29b0a8d0ed833868805001355cada42ed7597f91d1af7a01407",
    "bytes": 4880
  },
  {
    "path": "examples/software-factory/controller/src/lib/worker/client.ts",
    "gitBlob": "98ee0e811e91f13b6e7f6d7659db5311379bb5a4",
    "sha256": "195282a5a01e3c7dde235711f9710745195912572037a88638261621a705f374",
    "bytes": 10775
  },
  {
    "path": "examples/software-factory/controller/src/lib/worker/wire.ts",
    "gitBlob": "0c7696b4c0bbb0705aecbb272af40d3c3a1c8af6",
    "sha256": "2fd6ff07389bc72477552f5cedbc4189f314a1da2d0de9d23f6ed05c52643574",
    "bytes": 1970
  },
  {
    "path": "examples/software-factory/controller/src/lib/worker/sse.ts",
    "gitBlob": "471e351e293cd428f89d1a2e317112b94c897335",
    "sha256": "f7bbcb5c6eec64a9fccc98f8242e53d901e4c48b5739ee15dc5b359013ad19fc",
    "bytes": 2491
  },
  {
    "path": "examples/software-factory/controller/package.json",
    "gitBlob": "d2836575175fc979ad72a881291fdca104931304",
    "sha256": "f2f00f3395ec0389425465d67454032e3bcdbd1ffa21093aecdfc95a8bde2a52",
    "bytes": 1179
  },
  {
    "path": "examples/software-factory/controller/tsconfig.json",
    "gitBlob": "2b485dd21e8ab15249da95e41bfb2f158969e768",
    "sha256": "9b8f7479c2ecb20e03288d400fb3fbd1eef37014a1b2895bcc4131812212f37a",
    "bytes": 583
  },
  {
    "path": "package.json",
    "gitBlob": "75d1e52cf30aea7a0c41b55e9e083b421a575b39",
    "sha256": "9f965f5b995cd43350a5793948d74a61cf1acc4454f3ba41b557d0a286a11e45",
    "bytes": 5714
  },
  {
    "path": "pnpm-lock.yaml",
    "gitBlob": "b3f361cca058abd4029dd0e69c1e719f401963e1",
    "sha256": "46be0b081db024914c6da5d861c9416392abeea3bd44cbc10b6fba0840494ca0",
    "bytes": 625496
  },
  {
    "path": "tsconfig.json",
    "gitBlob": "4bb0f9ed220436e6205104021a82e24f6c0bf485",
    "sha256": "71a38113db8909197e9b8c0f58502050055feab24b595285546b3e6a5449fa81",
    "bytes": 300
  }
]

export function validatePinnedObservation(observation) {
  const refuse = () => { throw new ClientBundleError("client_bundle_pin_invalid") }
  if (observation?.commit !== pinnedCommit || !Array.isArray(observation.files) || observation.files.length !== expectedFiles.length) refuse()
  const actual = new Map()
  for (const record of observation.files) {
    if (!record || actual.has(record.path)) refuse()
    actual.set(record.path, record)
  }
  for (const expected of expectedFiles) {
    const record = actual.get(expected.path)
    if (!record || ["gitBlob", "sha256", "bytes"].some((field) => record[field] !== expected[field])) refuse()
  }
  return true
}

async function git(sourceRoot, args, encoding = "utf8") {
  const result = await execute("/usr/bin/git", ["-C", sourceRoot, ...args], { env: { TMPDIR: "/private/tmp" }, encoding, timeout: 10000, maxBuffer: 2 * 1024 * 1024 })
  if (result.stderr.length) throw new ClientBundleError("client_bundle_pin_invalid")
  return result.stdout
}

// Read-only Git and filesystem observations. No build, import or client call.
export async function verifyPinnedSource(sourceRoot) {
  try {
    if (typeof sourceRoot !== "string" || !path.isAbsolute(sourceRoot) || path.normalize(sourceRoot) !== sourceRoot || await realpath(sourceRoot) !== sourceRoot || !(await lstat(sourceRoot)).isDirectory()) throw new ClientBundleError("client_bundle_pin_invalid")
    const commit = (await git(sourceRoot, ["rev-parse", "HEAD"])).trim()
    if (commit !== pinnedCommit) throw new ClientBundleError("client_bundle_pin_invalid")
    const files = []
    for (const expected of expectedFiles) {
      const file = path.join(sourceRoot, expected.path)
      if (await realpath(file) !== file || !(await lstat(file)).isFile()) throw new ClientBundleError("client_bundle_pin_invalid")
      const bytes = await readFile(file)
      const gitBlob = (await git(sourceRoot, ["rev-parse", `${pinnedCommit}:${expected.path}`])).trim()
      const original = await git(sourceRoot, ["show", `${pinnedCommit}:${expected.path}`], "buffer")
      if (!bytes.equals(original)) throw new ClientBundleError("client_bundle_pin_invalid")
      files.push({ path: expected.path, gitBlob, sha256: digest(bytes), bytes: bytes.length })
    }
    const observation = { commit, files }
    validatePinnedObservation(observation)
    return observation
  } catch { throw new ClientBundleError("client_bundle_pin_invalid") }
}
