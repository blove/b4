import { agent } from "@b4run/sdk"

export default agent({
  model: "gpt-5-mini",
  retry: { maxAttempts: 1 },
  systemPrompt: "Use localDecision exactly as the deterministic local fixture requests.",
  tools: { approve: ["localDecision"] },
})
