import { createHash } from "node:crypto"

export function sha256(bytes) {
  return createHash("sha256").update(bytes).digest("hex")
}

// Only generated top-level thread-resource fields are normalized. Application
// metadata and route output retain the same keys and values even when their
// names resemble runtime fields. Raw bytes are always retained separately.
export function normalizeResponseBody(body, projection) {
  if (projection !== "thread") return body
  let value
  try { value = JSON.parse(body) } catch { return body }
  if (value === null || Array.isArray(value) || typeof value !== "object") return body
  if (typeof value.thread_id === "string") value.thread_id = "<generated-thread-id>"
  for (const key of ["created_at", "updated_at"])
    if (typeof value[key] === "string") value[key] = "<generated-timestamp>"
  return JSON.stringify(value)
}

export function parseSse(source) {
  const frames = []
  for (const block of source.replaceAll("\r\n", "\n").split("\n\n")) {
    let event = "message"
    const data = []
    for (const line of block.split("\n")) {
      if (line.startsWith("event:")) event = line.slice(6).trimStart()
      if (line.startsWith("data:")) data.push(line.slice(5).replace(/^ /, ""))
    }
    if (!data.length) continue
    const dataText = data.join("\n")
    let parsed = dataText
    try { parsed = JSON.parse(dataText) } catch { /* Keep non-JSON SSE data. */ }
    frames.push({ event, data: parsed, data_text: dataText })
  }
  return frames
}

export async function recordResponse(response, projection = "none") {
  const bytes = Buffer.from(await response.arrayBuffer())
  const body = bytes.toString("utf8")
  return {
    status: response.status,
    status_text: response.statusText,
    headers: [...response.headers.entries()],
    body_base64: bytes.toString("base64"),
    body_utf8: body,
    body_sha256: sha256(bytes),
    normalized_body_utf8: normalizeResponseBody(body, projection),
    ...(response.headers.get("content-type")?.includes("text/event-stream")
      ? { sse_frames: parseSse(body) }
      : {}),
  }
}
