import { appendFile } from "node:fs/promises"

export default async function localDecision(input: { label: string }): Promise<string> {
  const ledger = process.env.B4_LIFECYCLE_REFERENCE_LEDGER
  if (!ledger) throw new Error("Local lifecycle ledger is absent")
  await appendFile(ledger, JSON.stringify({ phase: "approved-tool", key: input.label }) + "\n")
  return "Local decision recorded."
}
