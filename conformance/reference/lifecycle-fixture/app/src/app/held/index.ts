import { appendFile, access } from "node:fs/promises"
import { join } from "node:path"

/** The application's own local effect is intentionally not abort-aware. */
export const workflow = async (input: { key: string }) => {
  const root = process.env.B4_LIFECYCLE_REFERENCE_BARRIERS
  const ledger = process.env.B4_LIFECYCLE_REFERENCE_LEDGER
  if (!root || !ledger || !/^[a-z0-9-]+$/.test(input.key)) throw new Error("Unexpected local barrier fixture input")
  await appendFile(ledger, JSON.stringify({ phase: "entered", key: input.key }) + "\n")
  const deadline = Date.now() + 15_000
  for (;;) {
    try { await access(join(root, input.key + ".release")); break } catch { /* Driver owns this fixture barrier. */ }
    if (Date.now() > deadline) throw new Error("Local fixture barrier deadline exceeded")
    await new Promise(resolve => setTimeout(resolve, 5))
  }
  await appendFile(ledger, JSON.stringify({ phase: "exited", key: input.key }) + "\n")
  return { completed: true, key: input.key }
}
