// Retain every nonempty SSE block, including retry-only and comment blocks.
// The raw body remains authoritative; these are separately derived wire facts.
export function parseLifecycleSse(source) {
  const blocks = []
  for (const text of source.replaceAll("\r\n", "\n").split("\n\n")) {
    if (!text) continue
    const lines = text.split("\n")
    const data = lines.filter(line => line.startsWith("data:")).map(line => line.slice(5).replace(/^ /, ""))
    if (data.length) {
      const dataText = data.join("\n")
      let value = dataText
      try { value = JSON.parse(dataText) } catch { /* Preserve non-JSON data. */ }
      const event = lines.find(line => line.startsWith("event:"))?.slice(6).trimStart() ?? "message"
      blocks.push({ kind: "event", event, data: value, data_text: dataText })
    } else if (lines.some(line => line.startsWith("retry:"))) {
      const retryText = lines.find(line => line.startsWith("retry:"))?.slice(6).trim()
      blocks.push({ kind: "retry", retry_ms: /^\d+$/.test(retryText) ? Number(retryText) : null, retry_text: retryText })
    } else blocks.push({ kind: "comment", lines })
  }
  return blocks
}
function messageIds(messages) {
  if (!Array.isArray(messages)) return
  for (const message of messages) {
    if (message?.lc === 1 && message.type === "constructor" && Array.isArray(message.id) && message.id.at(-1) === "HumanMessage" && typeof message.kwargs?.id === "string")
      message.kwargs.id = "<generated-human-message-id>"
  }
}
function pendingEntries(entries) {
  if (!Array.isArray(entries)) return
  for (const entry of entries) {
    if (typeof entry.interruptId === "string") entry.interruptId = "<generated-interrupt-id>"
    if (typeof entry.resumeKey === "string") entry.resumeKey = "<generated-resume-key>"
    if (typeof entry.grant === "string") entry.grant = "<generated-grant>"
    if (entry.value?.type === "permission-request") {
      if (typeof entry.value.interruptId === "string") entry.value.interruptId = "<generated-interrupt-id>"
      if (typeof entry.value.grant === "string") entry.value.grant = "<generated-grant>"
    }
  }
}
function checkpointValues(values) {
  messageIds(values?.messages)
  if (Array.isArray(values?.__pregel_tasks))
    for (const task of values.__pregel_tasks) messageIds(task?.args?.messages)
}
function checkpoint(value) {
  if (!value || typeof value !== "object") return
  for (const key of ["config", "parent_config"]) {
    const config = value[key]?.configurable
    if (typeof config?.checkpoint_id === "string") config.checkpoint_id = "<generated-checkpoint-id>"
  }
  checkpointValues(value.values)
  if (typeof value.metadata?.["b4:checkpoint-routes"]?.checkpointId === "string")
    value.metadata["b4:checkpoint-routes"].checkpointId = "<generated-checkpoint-id>"
  if (value.metadata?.parents && typeof value.metadata.parents === "object")
    for (const key of Object.keys(value.metadata.parents)) value.metadata.parents[key] = "<generated-parent-checkpoint-id>"
}
function wireData(event, data) {
  const value = structuredClone(data)
  if (!value || typeof value !== "object") return value
  if (event === "state") {
    if (typeof value.run_started_at === "string") value.run_started_at = "<runtime-timestamp>"
    if (typeof value.anchor === "string") value.anchor = "<generated-checkpoint-id>"
    pendingEntries(value.interrupts)
    checkpointValues(value.values)
  } else if (event === "interrupt" && value.type === "permission-request") {
    if (typeof value.interruptId === "string") value.interruptId = "<generated-interrupt-id>"
    if (typeof value.grant === "string") value.grant = "<generated-grant>"
  } else if (event === "done") messageIds(value.output?.messages)
  return value
}
export function projectLifecycleObservation(observation) {
  const response = observation.response
  const projected = { id: observation.id, status: response.status, capture: response.capture, ledger: observation.ledger }
  if (response.sse_blocks) {
    projected.blocks = response.sse_blocks.map(block => block.kind === "event"
      ? { kind: "event", event: block.event, data: wireData(block.event, block.data) }
      : block.kind === "retry" ? { kind: "retry", retry_ms: block.retry_ms >= 1500 && block.retry_ms <= 2500 ? "<retry-jitter>" : block.retry_ms }
      : structuredClone(block))
  } else {
    let value
    try { value = JSON.parse(response.body_utf8) } catch { value = response.body_utf8 }
    if (value && typeof value === "object" && !Array.isArray(value)) {
      const path = observation.request?.path ?? ""
      if (observation.id.startsWith("idle.") && typeof value.error?.message === "string") {
        const generatedId = /^\/threads\/([^/]+)/.exec(path)?.[1]
        if (generatedId) value.error.message = value.error.message.replaceAll(generatedId, "<generated-thread-id>")
      }
      if (/^\/threads(?:\/[^/]+)?$/.test(path)) {
        if (typeof value.thread_id === "string" && observation.id.startsWith("idle.")) value.thread_id = "<generated-thread-id>"
        for (const key of ["created_at", "updated_at"]) if (typeof value[key] === "string") value[key] = "<runtime-timestamp>"
      }
      if (path.endsWith("/state")) {
        if (typeof value.created_at === "string") value.created_at = "<runtime-timestamp>"
        checkpoint(value)
      }
      if (path.endsWith("/pending_interrupts")) pendingEntries(value.interrupts)
    }
    projected.body = value
  }
  return projected
}
