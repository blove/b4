import { appendFile } from "node:fs/promises"

/** Record one local fixture deployment after approval. No business API is called. */
export default async function deployProd(input: { env: string }): Promise<string> {
  const ledger = process.env.B4_GRANT_REFERENCE_LEDGER
  if (!ledger) throw new Error("Local fixture ledger is absent")
  await appendFile(ledger, input.env + "\n")
  return "deployed to " + input.env
}
